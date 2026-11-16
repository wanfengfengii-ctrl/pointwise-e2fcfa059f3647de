# LiveStore

LiveStore 用 Python 3.11 标准库采集有限录制源上的低延迟直播媒体。它实际轮询 HTTP 列表、并发下载完整段及 PART，把已证明完整的媒体段写入 SQLite，再通过独立连接按来源顺序读取正文和 SHA-256。它不解码或播放媒体。

## 运行

在项目根目录执行：

    python3.11 -B -m unittest discover -s tests -t . -v
    python3.11 -B -m livestore demo
    python3.11 -B -m livestore read --db archive.sqlite
    python3.11 -B -m livestore capture http://127.0.0.1:PORT/live.m3u8 --db archive.sqlite --polls 3

回归和演示需要允许绑定随机回环端口及在测试临时目录写 SQLite。无需安装依赖，不访问外部网站。演示使用 samples/recording.json，取得段40的首片后，让另一片及完整段等待门控；随后增量列表省略40，窗口推进到42并复用 URI。独立连接最终读取 (0,40)=A40B40、(0,41)=S41、(1,42)=S42，输出实际摘要、状态和 HTTP 轨迹。演示文件结束时清理。

## 公开接口

- livestore/playlist.py：Session.accept_playlist(text) 返回 Update(need_full, declarations, ended)。成功返回全部历史声明；上下文不足返回 need_full=True 和空声明，不接纳不明位置，但记录已观察的序号缺口。格式错误抛 PlaylistError，本次声明不生效。Session.declarations 是不可变快照；missing_ranges() 返回未知序号的半开范围。
- livestore/capture.py：Collector(url, archive, workers=8, timeout=10)。poll() 取得一次列表、必要时立即退回完整列表，接纳已完成响应并安排下载；不等待全部媒体。settle(timeout=5) 等待已安排工作，仍在途则抛 TimeoutError。再次 poll 重试失败项。wait_for_download(Position(sequence, discontinuity), part=None, timeout=5) 可等待一个已安排表示；None 指完整段，整数指从0起的分片。status() 无网络访问。控制方法由同一线程调用，工作线程只读取 HTTP。close() 取消未开始工作，等待有限超时的在途请求并接纳结果。
- livestore/archive.py：Archive(path=":memory:") 不存在时自动创建库。declare(declarations) 登记待补位置；publish(position, bytes) 供已经确认完整性的接纳方调用，相同正文幂等，冲突拒绝且保全旧正文。read() 返回 Snapshot(segments, pending)，各自按媒体序号排序；Segment 包含 position、原始 body 和 sha256，Pending 只有 position、reason。读取核对摘要，不把半段返回为正文。Archive 支持上下文管理，文件库可以通过独立连接读取已提交的段，事务内没有网络等待。
- livestore/source.py：Recording(stages) / Recording.from_json(path) 启动真实 ThreadingHTTPServer。Stage(playlist, resources, full_playlist=None) 的 resources 把路径映射为 Reply 元组；依次返回，耗尽后重复末项。不存在资源返回410。Reply(body, status=200, gate=None) 在请求到达时冻结，advance(index) 只影响未来请求。release(name) 放行响应，wait_requests/wait_responses 用条件变量等待实际轨迹；门控超时返回504。close 放行门控、关闭服务器并连接线程。录制控制只供测试，不是远程业务端点。

Position 参数为 (sequence, discontinuity)，identity 为 (discontinuity, sequence)。同一会话媒体序号不重置、不连续代次单调。新 Collector 开始新会话，不恢复旧采集状态；持久归档可以重开读取。进程崩溃恢复采集不在接口范围内。

## 有限列表与覆盖契约

接受 EXTM3U、MEDIA-SEQUENCE（缺省0）、DISCONTINUITY-SEQUENCE（缺省0）、DISCONTINUITY、EXTINF/URI、PART、SKIP、ENDLIST，以及 VERSION、TARGETDURATION、PART-INF、SERVER-CONTROL 中代码列出的有限元数据。未知 EXT 标签拒绝。无加密、字节范围、初始化映射、主播放列表、音视频同步或解码。下载仅允许 HTTP(S)。URI 是获取地址，不是媒体身份。

PART 时长须为正的有限十进制。同一列表内一个段的分片从位置0开始按序声明；后续列表可省略旧 PART 标签，已见前缀不会撤销，重叠位置时长不能改变。EXTINF 给出完整段总时长；只有已声明分片时长之和恰等于它时，分片集合才被证明齐备。下一段、ENDLIST 或窗口移出都不能自行封口。完整段正文等于全部分片按序拼接，来源保证同一位置不同地址内容不变。分片时长之和超过总时长时列表非法。

SKIP 省略此前已经声明的完整媒体段，MEDIA-SEQUENCE 仍指省略前窗口起点。须找到省略段及不连续上下文才能续接；缺少依据时请求不带 _HLS_skip 的完整列表，不猜 URI、正文或代次。常规轮询带 _HLS_skip=YES。窗口推进不撤销旧待补位置；回退列表已经前移，也要保留未知序号缺口。

媒体请求在发起时绑定不可变位置及段内位置。URI 复用不能改变响应归属。完整段与齐备分片是同一区间的替代表示，二者到达仍只发布一次；正文冲突抛错。有效分片跨轮询保留，失败不清空旧成果。当前实现保存整个响应及会话载荷，适用于有限录制，不承诺大媒体的内存上界。

complete 仅在已见 ENDLIST、没有待补或未知序号、最近列表有效且无待解决上下文时为真。冗余表示仍在途不影响已经证明的完整覆盖；download_errors 保留最近失败诊断，即使另一表示已完成同段。CLI capture 完成退出0、待补退出1、调用或存储错误退出2。

## 确定性回归

 tests/test_normal.py 核对完整列表、重复轮询、独立连接及重开读取、摘要与冲突保全；test_delta.py 核对省略续接、跨不连续边界、缺少上下文、格式拒绝、声明原子性及未知缺口；test_failures.py 核对完整回退、503重试、分片失效前保留、完整段失败时拼接和未齐尾段；test_coverage.py 核对齐备证明及完整段迟到后的单次发布；test_identity.py 核对跨代 URI 复用与同址不同位置。

期望由明确的原始字节和位置独立构造，不用采集输出作参照。门控控制因果顺序，超时仅用于报告事件未发生，不把性能计时当正确性条件。
