# 大媒体内存优化交接说明

本次改动只优化正文载荷的持有方式，不新增任何采集能力；播放列表协议、公开调用、
异常语义与库表结构均保持兼容。原有源码中 `playlist.py`、`source.py`、
`__main__.py`、`memory_bench.py`、`samples/` 与 `tests/` 原有文件字节未改。

## 改了什么

- 新增 `livestore/staging.py`：会话级磁盘暂存目录（`tempfile.mkdtemp`，落在基准
  重定向后的 `tempfile.tempdir`，即基准自己的临时目录内）。HTTP 工作线程以
  64KiB 定长块把响应体流式写入临时文件，同时增量计算 SHA-256；返回不可变
  `Ticket(position, part, path, size, digest)`。任何写入异常都会删掉半文件；
  `Content-Length` 已知时强制长度一致，短收仍然是失败，不会变成短段。
- `livestore/capture.py`：`_get` 的整段 `response.read()` 改为 `_download`
  流式落盘；`_bodies`（整段字节字典）改为 `_tickets`（票据字典）加 `_done`
  （已接纳票据的轻量标记集合，不含正文）。轮询、退回完整列表、绑定不可变位置、
  历史声明、齐备证明、待补原因、错误诊断、控制方法单线程等行为不变。
- `livestore/archive.py`：保留原 `publish(position, bytes)` 与 `read()`
  （显式读取仍返回完整正文）。新增 `publish_files()`，用
  `zeroblob` + 增量 `blobopen` 把暂存文件以 64KiB 块写进 SQLite，全程不拼整段；
  新增 `inspect()` 供 `status()` 使用，只取位置/原因并以流式 blob 重读校验摘要，
  损坏检测与旧行为一致（仍抛 `ValueError: stored media digest mismatch`）。
  旧库文件（同样的两张表、`user_version=1`）直接可读，见下方核验。

## 载荷持有与释放不变量

1. **在途**：工作线程同一时刻只持有 64KiB 读取块；响应体只存在于暂存文件。
2. **未封口分片**：票据与文件保留，窗口推进、来源 410、完整段失败都不清除；
   后续列表证明齐备后仍按原序拼接发布（`test_failures` 原断言继续覆盖）。
3. **发布成功**：事务提交后立即从 `_tickets` 移除并删除暂存文件，只在 `_done`
   保留无正文的 token 标记（用于避免重复下载、保持 `wait_for_download` 语义）。
4. **迟到替代**：位置已发布后到达的完整表示，或已滑出窗口的完整段，仍走
   `publish_files` 的逐字节核对（双方都流式、各自重算摘要，不只比摘要、也不因
   已发布而跳过）；一致即释放暂存，冲突抛错并保全旧正文。分片前缀在齐备证明
   成立前永远不构成完整替代，不会提前报冲突。
5. **异常**：暂存写入失败删半文件并记为下载错误，可在下轮 `poll` 重试；
   重读失败与提交前失败回滚整个事务，库中看不到半段、旧发布不受影响、暂存候选
   保留可重试（见 `tests/test_staging_failures.py` 的注入点说明）。
6. **close**：先 `shutdown(cancel_futures=True)` 并 `_drain` 接纳在途结果，再
   `shutil.rmtree` 整个会话暂存目录（含从未接纳的响应）；异常退出路径同样执行。
   只删除自建暂存目录，绝不触碰调用方数据库或其他文件。

## 实测结果（本机，Python 3.11.2，4 个工作者，冻结基准未改）

- `python3.11 -B -m unittest discover -s tests -t . -v`：33 个测试全部通过
  （原 26 个 + 新增 7 个暂存异常回归）。
- `python3.11 -B memory_bench.py target`：业务检查与冻结门槛全部通过。
  tracemalloc 峰值（字节，三轮量级一致）：
  - long 32MiB ≈ 0.87MiB，long 128MiB ≈ 0.87MiB（中位增量 ≈ 0，门槛 ≤4MiB）；
  - late 32MiB ≈ 0.55MiB，late 128MiB ≈ 0.55MiB。
  全部远低于每轮 ≤24MiB；4 倍规模增长近似为零。优化前同机 long 32MiB 峰值约
  67MiB（整段驻留内存）。
- `python3.11 -B memory_bench.py baseline`：同一流程不卡门槛，输出实际峰值，
  全部业务检查通过。
- `python3.11 -B diff_check.py`：独立参考模型 4 个差分用例全部通过
  （完整段+分片+迟到替代逐字节一致、历史分片跨窗口/410/503 保留、延迟冲突保全
  旧字节、close 后无暂存残留且数据库仍在）。
- `python3.11 -B -m livestore demo`：真实 ThreadingHTTPServer 录制，输出
  (0,40)=A40B40、(0,41)=S41、(1,42)=S42 的原始字节、摘要、状态与 HTTP 轨迹。
- 旧库兼容：用旧 DDL 手工建库写入后，新 `Archive` 的 `read()`/`inspect()`
  正常读出并校验摘要。
- 运行清单 `.pointwise-runtime.json` 的 `verify` 已按要求改为
  `["python3.11","-B","memory_bench.py","target"]`，`demo` 与其余字段不变。

## 新增文件与测试注入点

- `livestore/staging.py`（新）、`tests/test_staging_failures.py`（新，7 个用例）、
  `diff_check.py`（新，独立差分）、本文件。
- 注入点均为显式测试缝，生产路径默认关闭：
  - `Staging.write_fault(path, chunk_index)`：块落盘前抛 OSError，核验半文件
    清理、无半段可见、同绑定下轮重试成功；
  - `Staging.read_fault(path)`：暂存重读时抛 OSError，核验候选保留、旧发布保全、
    故障解除后同批票据发布；
  - `Archive.commit_fault(position)`：blob 已流式写入、提交前抛 OSError，核验
    事务回滚、无半行、跨会话旧库可读、候选可重试；
  - 另含短收检测、暂存目录整体回收与 close 后数据库保留核验。

## 已知边界（沿用原接口范围）

- 进程崩溃（SIGKILL）后的采集恢复不在接口范围内；崩溃可能在临时目录留下
  `mkdtemp` 暂存目录，正常 `close` 与异常清理路径均会回收。
- 显式 `read()` 按契约仍把完整正文载入内存，基准将其放在计量窗口外。
- 不做磁盘配额、读取器背压、媒体解码或跨进程采集。
