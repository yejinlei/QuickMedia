# QuickMedia 媒体服务器架构设计

版本：v0.3（决策定稿，作为开发 issue 的输入）
日期：2026-09-19
变更：v0.2 → v0.3 三个开放问题经业主拍板关闭（§9.1 / §9.2 / §9.3），并同步修订 §0 决策摘要、§1.3 非目标、§7.5 自研深度、§7.6 转码、§8 里程碑：
① **内核自研** —— L0（内存与调度）/ L2（容器与 codec 打包）/ L5（会话与流核心）三层 100% 自研，不引入第三方媒体内核；协议栈仍按 §7.5 分级。
② **内建 SIP** —— 自研 SIP 栈（UDP/TCP，RFC 3261/3262 + GB/T 28181 扩展），不再以 C 网关（opensips/kamailio）为默认路径。
③ **内建转码** —— 由"v2 可选项"升级为 v1.x 承诺能力，M2 首版落地；外部 FFmpeg worker 保留为可替换后端，不再是默认路径。
变更：v0.1 → v0.2 新增 §7.1.1 Rust 专项评估（回应评审意见"建议用 Rust 实现"），并据此更新 §0 决策摘要、M0 里程碑与 §9.3
前置输入：YEJ-178《ZLMediaKit 与 MediaMTX 对照调研报告》（下称"调研报告"）
交付形态：架构设计文档，作为后续开发 issue 的输入
范围外：编码实现、单元测试与压测脚本、管理后台 UI、计费与用户体系、部署与运维方案

---

## 0. 一页结论

| 问题 | 结论 |
|---|---|
| 语言 | **Go 1.24+**，单二进制交付。Rust 已按评审意见做专项评估（§7.1.1），结论为"当前约束下不采纳"，但给出 4 条可无审批重启评估的触发条件；峰值极小 UDP 包路径保留 cgo 逃逸口，不做双语言核心。 |
| 核心抽象 | `Path → Stream → Track → Unit`。Unit（≈codec unit / GOP 内最小可重打包单元）是唯一的媒体面货币，零拷贝引用计数。 |
| 面分离 | **数据面**只传 `Unit` 引用，不传协议字节；**控制面**（REST/OpenAPI + 内部 event bus）只做意图与状态查询。两侧通过 path 级订阅句柄解耦。 |
| 并发模型 | goroutine-per-connection 读循环 + path 级广播 worker + 有界 reader ring。不手动管理 epoll 线程池（Go 运行时已做），只在 UDP 组播大包路径上合并读循环。 |
| 扩展点 | 三类插件，统一注册表：**Adapter**（协议进出）、**Codec**（RTP/RTMP 打包）、**Capability**（录像/截图/转推/转码/录像回放）。内核不为任何具体协议留后门字段。 |
| 插件加载 | v1 为**编译期注册**（进程内）；**第三方/受授权协议**（NDI、国标网关）走**独立进程 + 本地 gRPC + 共享内存 Unit ring**，内核契约不变。 |
| 转码 | **内建**（业主拍板，M2 首版交付）。`TranscoderBackend` SPI 保留，内建后端探测 NVENC > QSV > VAAPI > libx264 降级；外部 FFmpeg worker 保留为可替换后端，不再是默认。GPU 生命周期、显存池与崩溃恢复按 §7.6 的隔离边界实施。 |
| SIP | **内建**（业主拍板）。自研 SIP 栈，SIP 核心进 M2、GB/T 28181 对接联调进 M3；C 网关降级为可选兼容路径（§9.1）。 |
| 状态 | 核心**有状态**（媒体面必须有），但把"可迁移状态"与"不可迁移状态"显式分表（§5.3），这是横向扩展的前提条件。 |
| 集群 | 三阶段：L4/L7 前置 → read replica（拉流溯源）→ 带会话迁移的源站组。**不做**同流多写者、不做 SFU 会议。 |
| 协议分级 | P0 11 项（RTSP/RTMPS、RTMP、HTTP-FLV、HLS+LL、WebRTC WHIP/WHEP、SRT、MPEG-TS、Raw RTP、MoQ、点播+seek）；P1 5 项（GB28181、ONVIF、HTTP-TS/fMP4、TS 透传、绝对时间戳固化）；P2 5 项（JT1078、DASH/CMAF 点播、NDI/ISUP、SFU/MCU、云存储直写）。 |
| 性能验收口径 | 单核 ≥1 万路 RTSP 只读广播、≥2 万 GOP/s 转协议扇出、≥5 千路 WebRTC 播放；单跳 WebRTC 端到端 <300ms、LL-HLS <1.5s；媒体面每 Unit 0 次 memcpy（OS 协议栈之外）。全部需在 M1 建立 `bench` 回归基线后才可对外宣称。 |

**开放问题关闭状态（v0.3，业主 2026-09-19 拍板）**：§9.1 内建 SIP（自研）；§9.2 内建转码（v1.x 承诺）；§9.3 内核自研（L0/L2/L5 全自研，C SDK 嵌入仍非目标）。三项均已转化为开发 issue，MVP 以 M0→M1→M2 推进。

---

## 1. 设计目标与非目标

### 1.1 硬约束（来自项目描述）

- **高性能**：单机承载大容量并发流，CPU/内存开销与 ZLMediaKit 同一量级或更优。
- **易扩展**：新增协议、编解码、能力（转码/录制/截图/转推）都是**加一个模块、不改内核**；单机到顶后可水平扩展到集群。

### 1.2 派生目标（本设计新增，需评审确认）

- **G3 可验证**：性能不是形容词。每个性能声明必须绑定指标定义、测量方法与基线数据（§4.5、§8）。
- **G4 上游可持续**：协议引擎优先复用 MIT 许可的上游库（bluenviron/*、pion、moq），自研只做上游不做的部分（单端口复用、国标、TS 透传、集群迁移）。自研越少，"同一量级"越容易守住。
- **G5 内核纯净**：内核代码中不出现任何协议名、编解码名、厂商名。反向检查项写入 CI（grep 门禁）。

### 1.3 非目标

| 非目标 | 理由 |
|---|---|
| 内建转码 | **承诺**（业主拍板，M2 首版交付）。工程投入与隔离边界见 §7.6 |
| WebRTC SFU/MCU 会议 | 与"一对多单向广播"的媒体面模型正交；需要全新的订阅图与混流器 |
| Web UI、计费、用户体系、租户 | 产品化范围，独立 issue |
| 部署与运维方案（K8s/Docker Compose/Helm/监控栈） | 独立 issue，但本设计预留指标与配置形态 |
| C SDK / 嵌入模式 | v1 不做；Go 无法提供 C ABI 嵌入。**已确认非硬需求**（业主拍板，见 §9.3），故不触发 §7.1.1 的 T1 重启条件 |
| MPEG-DASH/CMAF 点播（P2）、NDI/ISUP（P2） | 前者无上游实现，后者有授权问题 |

---

## 2. 总体架构

### 2.1 分层与模块边界

```
┌────────────────────────────────────────────────────────────────────┐
│ L6  Control Plane                                                   │
│   Control API (OpenAPI/REST) · Auth(JWT/HTTP/internal) · Hooks      │
│   Event Bus(内部) · Metrics(Prometheus) · pprof · Config hot-reload │
├────────────────────────────────────────────────────────────────────┤
│ L5  Session & Stream Core  ← 唯一"有状态"层                          │
│   PathManager · Path(状态机) · Stream · SubStream(按 track 分叉)    │
│   UnitRemuxer · 广播/背压 · 绝对时间戳(NTP↔RTP) · always-available  │
│   ReaderRing(有界) · 源内容缓存(GOP/PacketCache)                    │
├────────────────────────────────────────────────────────────────────┤
│ L4  Capability Layer  ← 插件入口 B（录音/截图/转推/转码/回放）       │
│   Capability 注册表 · 生命周期钩子 · TranscoderBackend SPI(外部)    │
├────────────────────────────────────────────────────────────────────┤
│ L3  Protocol Adapters  ← 插件入口 A（薄适配 from_stream / to_stream）│
│   RTSP · RTMPS · RTMP · HTTP-FLV · HLS · WebRTC · SRT · TS ·       │
│   RawRTP · MoQ · [P1] GB28181 · [P1] ONVIF · [P2] Gateway          │
├────────────────────────────────────────────────────────────────────┤
│ L2  Container / Framing                                           │
│   ES · PS · MPEG-TS · FLV · fMP4/CMAF · KLV       插件入口 C(codec)│
│   Codec Registry：H.264/265 · H.266 · AV1 · VP8/9 · M-JPEG ·       │
│   AAC · Opus · G.7xx · MP3 · AC-3 · LPCM · SVAC              │
├────────────────────────────────────────────────────────────────────┤
│ L1  Transport                                                     │
│   TCP · UDP(+multicast) · QUIC/H3 · WebTransport · TLS             │
│   单端口复用(multiplexer) · 连接迁移(conn-migration) · ICE/STUN/DTLS│
├────────────────────────────────────────────────────────────────────┤
│ L0  Memory & Scheduling                                           │
│   Unit 引用计数 · size-class 对象池 · 无锁 ring · UDP GRO/批量读    │
└────────────────────────────────────────────────────────────────────┘
```

**边界规则（写进代码评审清单）：**

1. L3 适配器**只能**引用 L5 的 `Stream` 接口与 L2 的 codec registry，不得直接触碰 L5 内部结构（禁止 import `core/internal`）。
2. L4/L3/L2 插件**不得**持有 path 生命周期；只能持有订阅句柄（`Subscription`），由内核授予与回收。
3. L5 不知道任何具体协议：广播 worker 只操作 `Track + Unit`。
4. L1 是唯一能 `listen()` 的地方；单端口复用只在 L1 实现，L3 通过 `Listener` 注入拿连接。
5. 依赖方向严格单向向下（L3→L5→L2→L1）。反向一律用回调/接口注入，禁止包级反向 import（CI `go vet` + 自定义 importlint 门禁）。

### 2.2 核心数据结构（契约级定义）

```go
// Unit —— 媒体面的唯一货币。不可变，引用计数，禁止就地改写 payload。
type Unit struct {
    TrackID    TrackID
    Codec      CodecID      // h264/h265/aac/opus/...
    Kind       CodecKind    // video/audio/data
    Payload    []byte       // ES 级（已去容器、已去 RTP 头）
    PTS        Time         // 绝对时间（NTP 锚定），0 = 未知
    DTS        Time
    Duration   Duration
    Key        bool         // 关键帧 / 随机接入点
    Sequence   uint64       // track 内单调递增
    Flags      UnitFlags    // Discontinuity / CEA608 / KLV ...
    Refs       atomic.Int32 // 引用计数，池化回收用
}

type Track struct {
    ID       TrackID
    Codec    CodecID
    Params   map[string]string   // SPS/PPS 等以 key-value 传递，避免结构体膨胀
    Bandwidth uint64
    Timescale uint64
}

type Stream struct {          // 一个 path 的内容
    Tracks []Track
    // SubStream 派生：按 track 集合分叉（多画面/仅音频）
}

// L3 与 L5 之间唯一的数据面接口
type StreamReader interface { ReadUnit() (*Unit, error) }   // error = EOF/超时/中断
type StreamWriter interface { WriteUnit(*Unit) error }      // 仅 publisher 使用

// L5 授予给插件的句柄（能力层与协议层共用）
type Subscription struct { /* reader + 订阅统计 + 取消 */ }
```

**为什么是 Unit 而不是 Frame/字节流**：调研报告 §3.2 指出 `unit_remuxer` 让"不重编码换容器/换协议"成为可能。Unit 粒度（而非整 GOP、也非单包）是零拷贝与按需解复用的分界线：

- 比 GOP 细：慢消费者可以跳到下一个 Unit 而不是下一个 GOP，掉帧代价小。
- 比 RTP 包粗：TS/FLV/fMP4 的 remux 不需要逐包重组，CPU 开销与"解码"解耦。

**偏离基线说明**：ZLMediaKit 用 `Frame + Track + MediaSource`，语义等价，字段命名与 MediaMTX 的 unit 模型对齐（调研报告 §3.1/3.2）。此处选择不是折中，是采纳 MediaMTX 侧已被 `sub_stream.go`、`offline_sub_stream*` 验证的粒度。

### 2.3 数据面与控制面分离

| | 数据面 | 控制面 |
|---|---|---|
| 传输什么 | `Unit` 引用（进程内）、协议字节（跨进程） | JSON/RPC 意图与状态 |
| 触发方式 | 推（publisher → 内核 → reader） | 请求/响应 + 事件推送 |
| 吞吐量级 | 百万 Unit/s 级 | 个位数 ~ 百 ops/s |
| 失败语义 | 丢包/丢帧/退订，**不阻塞**控制面 | 幂等，可重试，**不感知**帧 |
| 谁能触碰 | 仅 L1–L5 | 外部调用方 + L4 capability |

**硬约束（防止退化）**：

1. 控制面请求**不得**遍历 path 列表时持有广播锁；状态快照走 `Sync/Async` 双接口（`paths` 实时、`paths/stats` 走采样）。
2. 任何控制面接口不得接收或返回媒体负载（截图/录像等二进制产物走独立对象存储 + URL 返回，不进 JSON）。
3. Event Bus 只做**通知**（path 创建/销毁/读者增减），不做**命令**；命令走显式接口。
4. Metrics 采样不得在广播热路径上分配对象（预注册 gauge + atomic 计数）。

**偏离基线说明**：ZLMediaKit 的 `WebApi.cpp`（119KB）把 API、鉴权、hook 触发混在一层，是调研报告点名的可维护性风险。本设计把控制面收敛为"OpenAPI 定义 → 生成 → handler 只做校验与转调"，且要求 OpenAPI 文件是 API 的唯一真相来源（生成式文档，禁止手写文档漂移）。

---

## 3. 会话与流管理

### 3.1 Path 状态机（单发布模型）

```
        publish(ok)                subscribe(1st)
idle ──────────────► publishing ─────────────────► published(subscribed)
 ▲                        │  │                          │
 │            publisher lost  │ no subscribers            │ readers=0
 └────────────────────────────┴──────────────────────────┘
        (retain=T, 过期后释放订阅；有 always-available 内容时回到 idle+ready)
```

- **单发布者**：一个 path 同一时刻只接受一个 publisher。第二个 publisher 返回 `409 Conflict`，不做混流（调研报告 §3.3 的行业共识）。
- **retain 窗口**：publisher 断开后保留内容 T（默认 5s，可配），用于重连平滑；超过 T 且无 reader 时释放。
- **always-available**：有录像内容时 path 永不真正空（用离线段无缝拼接），这是安防场景刚需，且不需要重编码。
- **子流（SubStream）**：按 track 分叉出独立可寻址内容（如 `cam1/video0`、`cam1/audio0`），解决"同一码流多轨、不同客户端只要其中一轨"的带宽浪费。

### 3.2 广播与背压

- 每 path 一个广播 worker，遍历订阅者列表；订阅者持**有界 ring**（默认 512 Unit，按带宽动态缩）。
- 慢消费者策略，三档可配：`drop-toward-key`（丢到下一个关键帧）/ `drop-gop`（跳过整 GOP，默认，延迟最优）/ `pause`（阻塞 publisher，仅用于录制等强一致能力）。
- 退订触发条件：ring 连续满 `N` 次（默认 4）或 reader 心跳超时 → 主动断连并记录 metric（`slow_consumer_dropped_total`）。这保证单个失控客户端不拖垮整条 path。

### 3.3 绝对时间戳（P0）

- 内核维护一个 `NTPEpoch`（NTP ↔ RTP 换算锚点），全部 track 的 PTS 在写入时归一化为绝对时间。
- 出口映射：HLS `EXT-X-PROGRAM-DATE-TIME`、RTSP/WebRTC RTCP Sender Report、fMP4 `mfhd baseMediaDecodeTime`。
- 源缺失 PTS 时按 track 内顺序 + Duration 补算，并置 `Flags.Discontinuity` 标记边界（下游可据此跳变）。
- 用途：回放对齐、跨路同步（多画面）、CDN 缓存命中一致性。**不做**时钟漂移修正（超出必要范围，客户端自行补偿）。

### 3.4 有状态设计与可导出状态

媒体服务器本质有状态（订阅图、缓冲、时间戳锚点），因此本设计不假装无状态；但把状态显式分成两类，这是 §5.3 集群迁移的前提：

| 类别 | 内容 | 可迁移性 |
|---|---|---|
| **S1 可迁移** | path 元数据（配置、codec 参数、绝对时间锚点）、订阅表（reader 清单）、录像索引 | 可通过控制面导出/导入 |
| **S2 不可迁移** | reader 的 TCP/UDP/QUIC 连接、UDP 组播成员关系、SRT 握手、WebRTC DTLS | 只能让客户端重连 |
| **S3 派生** | GOP 缓存、ring buffer、解码状态 | 丢失可接受，靠关键帧恢复 |

**结论**：迁移 = 导出 S1 + 拉取内容 + 通知客户端重连（S2）。这是 §5.3 三阶段路线的技术底线，也是"不做同流多写者"的原因。

---

## 4. 高性能路径

### 4.1 线程/调度模型

**采纳**：goroutine-per-connection 读循环 + path 级广播 worker + 有界 reader ring（MediaMTX 模型）。

**为什么不手动管理 epoll 多线程池（ZLMediaKit 模型）**：

1. Go 运行时已把 epoll + 工作线程 + 抢占做掉，手写等于重复实现且失去 GC/调度收益。
2. ZLMediaKit 的多线程调度是为 C++ 无 GC 的堆分配成本设计的；Go 的 per-unit 分配走对象池后，热点在系统调用而非分配器（调研报告 §4：pprof 显示热点在 `syscall.recvfrom`，非 GC）。
3. 维护成本：连接迁移、NUMA、线程亲和都需要持续投入，且收益被 Go 的调度摊薄。

**保留逃逸口**：单 UDP 端口收到高密度组播时，多个 goroutine 各自 `recvfrom` 会放大系统调用开销。因此 L1 提供 `UDPMux`（单读循环 + 按 `(addr, session)` 分组派发 + 批量 64KB 读），并在编译期开启 UDP GRO（`GODEBUG=udp_gro` / cgo 设置 `UDP_GRO`）。这是唯一的"手动优化"位置，其余一律走运行时。

**并发上限约束**：连接数上限由 `rlimit`（fd）+ 每连接 goroutine 栈预算决定，默认目标 20 万 fd / 单机。每连接常驻内存目标 ≤ 24KB（含读缓冲、ring 指针），这是"单机大容量"的主要内存预算项。

### 4.2 零拷贝与内存池

| 环节 | 策略 | memcpy 次数 |
|---|---|---|
| 网络收包 → Unit | 直接进 size-class 池对象；UDP 用批量读减少 syscall | 0（OS 协议栈之外） |
| Unit → 各协议出口 | 同一 `Payload` 引用，出口各自打包（RTP 头/TS 包头在**独立小池**中构造，不复制负载） | 0 |
| Unit → 录像 | fMP4/TS muxer 追加写；页对齐时用 `mmap` + 写偏移，避免读回 | 0（追加） |
| Unit → 订阅者 ring | 传引用，不拷贝 | 0 |
| 跨进程（第三方网关/转码） | 共享内存 ring + 引用计数（Unix domain；Windows 命名映射） | 0 |
| Unit 回收 | 引用计数归零 → 归还 size-class 池；size class 上限 256KB，超出走 fallback 分配 | — |

**验收口径**：以 pprof `runtime.memeasyget`/自建插桩统计每 Unit 媒体面 memcpy 次数，目标 **0**（RTP/TS 封装头部构造不计）。这是唯一可审计的"零拷贝"证据，不接受"我们没写 memcpy"式声明。

**不做的事**：io_uring（Go 运行时未原生支持，收益需先有 syscall 热点证据）、DPDK/eBPF（超出本项目量级，见 §7.2）。

### 4.3 硬件加速与降级路径

**v1 结论（v0.3 更新）：内核不含编解码数据面，转码由 L4 能力层承载。** 硬件加速对**媒体面（推流/拉流/广播）**仍然不需要——这条不变。内建转码的硬加速选择是 capability 的内部问题：

```
TranscoderBackend SPI（L4 Capability 层，内核不实现）
   ├── v1: builtin 后端（M2 交付）—— 探测 NVENC > QSV > VAAPI > libx264，按 GPU 可用性降级
   │        降级链在能力启动时确定并上报能力清单，运行中不可切换（避免中途重建管线）
   └── v1+: FFmpegWorker —— 进程外后端，保留为可替换实现（降级路径 / 高吞吐批处理）
```

**为什么不在内核里放 NVENC 直连**：这会立刻把 GPU 生命周期、显存池、会话并发上限、驱动崩溃恢复拉进内核热路径，与"内核纯净"（G5）冲突。内建转码通过"Capability + 后端 SPI"承载——编码器代码在 `capabilities/transcoder/`，L5/L2 不感知编码器存在（隔离边界见 §7.6）。转码故障最多丢失转码会话，不会拖垮媒体面。

### 4.4 单端口复用与连接迁移（自研，差异点）

- **单端口**：TCP/UDP/QUIC 共用一个 `(host, port)`，由 L1 的 multiplexer 按首包识别协议并移交对应适配器。价值：NAT/云环境只暴露一个端口、k8s Service 定义简化、WebRTC 候选收敛。
- **连接迁移**：UDP 会话在网络变化后凭 `(cookie, session-id)` 重绑定到新地址（调研报告 §2.1：ZLMediaKit 标注为"开源界唯一"）。价值：移动客户端/车载场景（JT1078 生态）不掉流。
- **实现位置**：L1 的 session registry，与 ICE 无关（ICE 只管候选，迁移管地址变化）。
- **风险**：单端口复用会放大"错误首包"的解析成本，必须做首包长度与魔数预检（≤64B），失败即关闭，不计入会话。

### 4.5 可测性能目标（口径 + 测量方法）

**口径定义（先定义，后承诺）：**

| 指标 | 定义 | M1 基线目标 | 测量方法 |
|---|---|---|---|
| 只读广播并发 | 同一 path、同 codec、单节点、无转发时的 reader 数 | 单核 ≥ 1 万路 RTSP-over-TCP | `bench forward`：1 publisher → N RTSP readers，固定 720p@30 GOP=2s |
| 转协议扇出吞吐 | 单位时间内 publisher→内核→reader 转换的 GOP 数 | ≥ 2 万 GOP/s/核 | `bench remux`：RTMP in → HLS+RTSP+WebRTC out |
| WebRTC 并发 | 单节点 WHEP reader 数 | ≥ 5 千路 | `bench webrtc`：headless 客户端，固定 ICE 成功 |
| 单跳延迟 | publisher 出帧到 reader 收帧的端到端中位数（P50/P95） | WebRTC < 300ms / LL-HLS < 1.5s / RTSP-TCP < 500ms | 打时间戳 Unit（`Flags` 载荷标记）+ 客户端回报 |
| 每 Unit 拷贝 | 媒体面 memcpy 次数 | 0 | pprof + 插桩计数器 |
| 内存/连接 | 常驻内存 per connection | ≤ 24KB | pprof heap 差分 |
| 广播延迟抖动 | reader 收到 Unit 的间隔方差 | P95 抖动 < 20ms（1000 readers） | 客户端采样 |

**硬性要求**：
1. M1 交付 `bench` 子命令 + 上述 7 项基线数据（数值可以是"实测值"而非目标值）。
2. 对外宣称性能数字前，必须附上：硬件配置、codec/分辨率/GOP 参数、客户端类型、测试脚本 commit SHA。无证据的数字一律写"目标"而非"实测"。
3. 基线数据进入 CI 回归：任一指标退化 >10% 触发告警（不阻塞合并，但必须解释）。

---

## 5. 易扩展机制

### 5.1 三类插件，一个注册表

```go
// 插件统一元信息（内核只依赖这一份）
type ModuleInfo struct {
    Name       string        // 全局唯一
    Version    semver.Version
    Type       ModuleType    // Adapter | Codec | Capability
    MinKernel  semver.Version
    Priority   int           // 同 Type 多实现时的选择顺序
}

// A. 协议适配器：既是 source 也是 sink，但能力可分别声明
type Adapter interface {
    ModuleInfo() ModuleInfo
    SupportsScheme(scheme string) bool
    NewSource(req SourceRequest) (StreamWriter, error)     // 客户端推/外部拉入
    NewSink(req SinkRequest) (StreamReader, error)         // 服务端推给客户端
    Hooks() AdapterHooks                                    // OnClose/OnError/OnBandwidth
}

// B. Codec：负责 ES ↔ RTP/RTMP/TS 的打包与解包
type CodecPackers interface {
    ModuleInfo() ModuleInfo
    RTPPacker() (RTPPacker, error)                          // 含 FU-A 分片策略
    RTPUnpacker() (RTPUnpacker, error)
    RTMPPacker() (RTMPPacker, error)                        // 可选：不支持即返回 ErrUnsupported
    ContainerPackers() []ContainerPacker                    // TS/FLV/fMP4
}

// C. Capability：挂在订阅或 path 上的横向能力
type Capability interface {
    ModuleInfo() ModuleInfo
    OnAttach(sub Subscription, cfg Config) (CapabilityHandle, error)
    OnDetach(h CapabilityHandle) error
    ConfigSchema() ConfigSchema   // 供控制面自动生成配置校验
}
```

**"加模块不改内核"的具体含义（可核验）：**

1. 新增协议 = 新增一个包实现 `Adapter`，并在 `init()` 注册（编译期）。内核 diff 应为 **0 行**。
2. 新增 codec = 实现 `CodecPackers` + 注册；同时更新能力矩阵（用于协商）。
3. 新增能力 = 实现 `Capability`；其配置通过 `ConfigSchema` 自动进入控制面校验与文档。
4. 内核新增字段/开关必须走设计变更评审；CI 有 grep 门禁（内核目录禁止出现协议缩写：`rtsp|rtmp|hls|srt|moq|gb28181`）。

**验收口径（易扩展的量化定义）**：
- 新增一个 P0 级协议适配器（有上游库可依赖时）：**≤ 3 人日**；无上游库（自研栈）：**≤ 15 人日**，且内核 diff = 0 行。
- 新增 codec 的 RTP/RTMP 打包对：**≤ 1 人日**。
- 新增能力（如截图）：**≤ 2 人日**。
上述工时在 M2 阶段用"内部演练任务"实测（真实写一个玩具协议，如 HTTP raw ES），取实测值替换估计值。

### 5.2 协商机制

- **能力矩阵**：每个 codec 的 RTP/RTMP/TS/FLV/fMP4/WebRTC 支持性以声明式表维护（而非散落在代码分支）。
- **协商规则**：source 与 sink 取交集；交集为空时**不静默失败**，返回显式 `ErrNoCommonProfile` 并带双方能力列表（便于运维定位）。
- **降维而非转码**：交集为空但同为视频时，允许"track 丢弃"（如多轨流只推第一轨），不允许隐式转码。
- **浏览器友好档**：WebRTC 出口默认拒绝 H.265 与含 B 帧的 H.264（调研报告 §7.3 的硬伤），返回显式错误 + 建议，而不是静默失败。

### 5.3 横向扩展路线（三阶段，含前提条件）

| 阶段 | 拓扑 | 前提条件 | 迁移能力 |
|---|---|---|---|
| **E1** | 单节点 + L4 前置（RTSP/RTMP/SRT 用四层；HLS/WebRTC 用 L7 + sticky） | 无 | 无（节点故障即断流） |
| **E2** | read replica：reader 侧节点按需向 source 节点拉流（溯源），source 节点只持 publisher | publisher 侧与 reader 侧流量比确定；拉流带宽可承受 | 无，但 source 节点故障只影响一次重建 |
| **E3** | source 组 + 会话迁移：S1 状态可导出，故障转移时 reader 重连到新节点并从 GOP 缓存恢复 | **必须先完成**：S1 导出/导入 API、GOP 缓存持久化、绝对时间戳跨节点一致、客户端重连 SLA 定义 | 有（客户端重连 + 从关键帧恢复，允许 ≤1 GOP 空洞） |

**明确不做**：同流多写者（会导致时间戳与关键帧语义冲突）、跨节点 SFU 混流、无客户端感知的透明迁移（S2 不可迁移，见 §3.4）。

**为什么不做"透明迁移"**：媒体面的 UDP/QUIC 连接状态（S2）无法复制；做到无感知需要给每个 reader 建一个反向控制通道并在节点间同步播放位点，复杂度接近一套 SFU，且收益仅在极端场景。因此把迁移设计为"快速可恢复"（目标：source 节点故障 → reader 恢复播放 ≤ 3s），而不是"无感"。

### 5.4 第三方/受授权协议的进程边界（插件形态二）

编译期注册解决"内部团队加模块"，但解决不了三类需求：NDI（授权 SDK）、国标网关（可能用 C 的 kamailio/opensips）、商业闭源协议。为此定义**进程外网关**契约：

```
kernel  ──(gRPC: 控制/订阅)──  gateway 进程
kernel  ──(共享内存 Unit ring + UDS 唤醒)──  gateway 进程
```

- 控制面走 gRPC（意图与状态），数据面走共享内存 ring（零拷贝，跨进程）。
- **适用范围（v0.3 收敛）**：NDI（授权 SDK）、商业闭源协议，以及 SIP 的可选兼容路径（C 网关 opensips/kamailio，仅作回退，默认走内建 SIP，见 §9.1）。
- gateway 只实现 `Adapter` 语义的子集（通常是纯 source 或纯 sink），能力在 manifest 中声明。
- 失败隔离：gateway 崩溃只丢失自己的连接，内核 path 不降级。
- **这是唯一的跨进程数据面通道**，其余插件一律进程内（避免 IPC 开销进入热路径）。

---

## 6. 协议范围分级

分级依据：上游库可用性（降低"加模块"成本）、场景刚需、工程不确定性。

### P0（首版必须，M1–M2）

| 协议 | 方向 | 依赖 | 备注 |
|---|---|---|---|
| RTSP / RTSPS | 收/发 | gortsplib | UDP/TCP/multicast + TLS + MPEG-TS 载荷 |
| RTMP / RTMPS | 收/发 | gortmplib | enhanced-RTMP（H.265/Opus） |
| HTTP-FLV / WS-FLV | 收/发 | 自研薄层 | 浏览器兼容兜底，实现成本低 |
| HLS（mpegts + fMP4，含 LL-HLS） | 收/发 | gohlslib | CDN 前置 + 签名鉴权 |
| WebRTC（WHIP/WHEP） | 收/发 | pion | TWCC/NACK/RTX；**单端口复用 + 连接迁移自研** |
| SRT | 收/发 | gosrt | 加密 + NAK |
| MPEG-TS over UDP/multicast/Unix socket | 收/发 | 自研 | Unix socket 优先（比 UDP 省） |
| Raw RTP over UDP（SDP 描述） | 收/发 | 自研 | 安防/国标前置必需 |
| 点播 + seek（MP4/fMP4，RTSP/RTMP/FLV） | 发 | 自研 | 安防回放刚需 |
| **MoQ（Media-over-QUIC）** | 收/发 | moq | 差异化抓手，两家基线中仅 MediaMTX 有 |
| **绝对时间戳全链路** | — | 自研 | NTP↔RTP 锚点 + 各出口映射 |

### P1（紧随，M3）

| 协议/能力 | 补齐方式 | 备注 |
|---|---|---|
| GB/T 28181（SIP + PS + RTP，含双向对讲、主动拉流、级联） | **内建 Adapter + 内建 SIP 栈（自研）** | SIP 核心进 M2，GB28181 对接进 M3；见 §9.1（已拍板） |
| ONVIF / PSIA | 内建 Adapter（WS/TS 两种） | ZLMediaKit 已验证可行 |
| HTTP-TS / WS-TS / HTTP-fMP4 | 内建 Sink（薄适配） | 客户端少但安防常用 |
| **TS 透传直出**（不解封装） | 内核新增 `PassthroughMode` | 多路分发省 CPU 的关键；两家开源基线共同缺口 |
| 录像 fMP4 + 录像回放（always-available 拼接） | 内建 Capability | |

### P2（可选 / 外部组件补齐）

| 项目 | 补齐方式 | 决策 |
|---|---|---|
| JT/T 1078 | 商业发行版或社区共建 | 默认放弃开源版承诺 |
| MPEG-DASH / CMAF 点播 | P2 内建（基于 fMP4 容器复用，CMAF 与 LL-HLS 同栈） | 无上游库，工程量大 |
| NDI / 3G-SDI-over-UDP | **进程外网关**（授权 SDK） | 授权问题，不承诺 |
| ISUP / 10151-30113 | 进程外网关 | 极少实现，不做 |
| WebRTC SFU / MCU | 不做 | 与广播模型正交，非目标 |
| 云存储直写（S3/MinIO） | 内建 Capability（存储抽象） | 商业版常见需求 |
| 内建转码 | **已升为承诺能力（原列于此，v0.3 移除）**，见 §7.6 与 §9.2 | 不在 P2 |
| HDR/360/边缘渲染 | 元数据管道，暂不做 | 无明确需求 |

**总口径**：P0 覆盖 11 项，覆盖两家基线全部直播协议 + MoQ；P1 补国标安防；P2 明确"不默认内建"。这与项目描述中"支持市面上所有多媒体协议"的差距在 §9 中列为需要产品确认的开放问题——**技术上"全部内建"不成立**，本设计给出的是"分级 + 进程外网关"的可交付替代方案。

---

## 7. 关键取舍记录（Decision Log）

每条格式：选项 → 结论 → 放弃项理由 → 反悔成本。

### 7.1 语言/框架选型
- **选项**：Go / C++ / Rust / 混合（C++ 核心 + Go 控制面）
- **结论**：**Go**，协议引擎复用 bluenviron/*、pion、moq。
- **放弃理由**：
  - C++：峰值性能略优且可嵌入 C SDK，但需自建 WebRTC 栈（ZLMediaKit 为 ICE 81KB + SDP 77KB + DTLS 45KB 起步的技术债）且无 MoQ 生态。
  - Rust：内存安全 + 性能，但缺生产级 RTSP/RTMP/GB28181/MoQ 库，等于把"复用上游"的收益清零；**完整评估（收益、成本、反悔成本、重启触发条件）见 §7.1.1**，结论为当前约束下不采纳。
  - 混合：双栈维护成本翻倍 + IPC 开销进入热路径，且"高性能"约束对 Go 不构成硬障碍（pprof 证据：热点在 syscall 非 GC）。
- **反悔成本**：**高**。语言是不可逆决策；若日后必须支持 C SDK 嵌入，需另起内核（预计 6–12 人月），因此 §9.2 要求尽早确认。

### 7.1.1 Rust 专项评估（回应评审意见）

评审意见："建议用 Rust 实现"。已专项评估，结论：**当前约束下不采纳 Rust，维持 Go**；但 Rust 是本方案最有力的候选者，Go 的决定不是"Rust 不行"，而是"Rust 的收益在本项目的约束集里被抵消，而它的成本是真实且不可逆的"。评估口径如下，任何一条成立即可重启本条决策（§9.3）。

**Rust 相对 Go 的真实优势（不争议的部分）**

| 维度 | 差距量级 | 对本项目是否关键 |
|---|---|---|
| 热路径 CPU 吞吐 | 持平或略优（0.9x–1.2x） | 不关键。本设计的性能约束是"同一量级或更优"而非"更快"，且 §4.1 已论证 Go 的热点在 `syscall.recvfrom` 而非 GC |
| GC 停顿 | 无 GC，可给硬抖动预算（<1ms） | **仅对"绝对时间戳 + 抖动 P95 <20ms"这一条间接相关**；10K reader 的广播抖动预算远大于 GC 停顿量级 |
| 内存可控性 | size-class 池 + `#[no_gc]`/引用计数可精确定额 | 关键但非决定性。Go 的 `sync.Pool` + 对象池（§4.2）已把 per-unit 分配压到 0，内存预算以 pprof 差分为验收口径，可控 |
| 崩溃安全（单点失败不炸全进程） | Go 的 goroutine panic 同样不炸进程，Rust 的 panic unwind 语义等价（`catch_unwind` 只覆盖 FFI 边界，媒体面用不到） | **等价，不构成差异**。这也是"Rust 更稳"这个常见论点在本项目不成立的原因——本设计的崩溃隔离靠订阅句柄与 gateway 进程边界（§5.4）实现，与语言无关 |
| 部署 | 静态单二进制，无运行时依赖 | 与 Go 完全等价，**不构成差异** |

**Rust 的真实成本（决定性的三条）**

1. **协议栈复用收益归零**。§7.5 选择"分级自研"的根本理由是 P0 的 11 项协议有成熟上游（bluenviron/* 一栈覆盖 RTSP/RTMP/HLS/SRT/MoQ，pion 覆盖 WebRTC，moq 覆盖 MoQ），单条协议接入 ≤3 人日。Rust 侧对应物要么不存在、要么不成熟、要么已停滞（截至 2026-09 的知识状态，评审时须复核 crates.io / 各仓库活跃度）：
   - RTSP：无生产级服务端实现，须自研（10–20 人日）
   - RTMP enhanced（H.265/Opus）：无可用 crate，须自研
   - MoQ：`moq-rs` 为研究/实验阶段，**P0 差异化项变成自研项**
   - WebRTC：`str0m` 为"无媒体处理的可靠传输层"，缺 DTLS/SRTP/TWCC/NACK/RTX 全套实现，补齐工作量约 40–80 人日；`webrtc-rs` 长期处于实验状态、多作者并行重写、不稳定
   - GB28181/SIP：Rust 侧同样无生产级栈，与 Go 打平（Go 也须自研或走 gateway），**Rust 在此不获得优势**
   - HLS/DASH：`mp4amphibian-rs` / `cmaf` 停滞或实验级
2. **团队与招聘现实**。可用工程师池、外包/开源社区贡献者基数、出问题时可参考的 stack overflow / GitHub issue 生态，Go 均为 Rust 数倍。媒体服务器是长期运维型产品，招聘漏斗宽度的成本在 6–12 个月的迭代周期里比性能差异更先显现。
3. **反悔成本极高**。语言是不可逆决策。若 Rust 内核先行、日后因客户需求必须补 C SDK 嵌入（§9.3）或发现协议栈缺口拖死 M1，则 6–12 人月的重写成本，且 Go 侧的现成上游库无法平移。**"先 Go，日后迁移到 Rust"在工程上不成立**，不存在渐进路径。

**反悔成本：高（同 §7.1）。** 结论：维持 Go。**但本条决策的失效条件明确**，任一成立即重启评估，不保留"以后再说"：
- (T1) C SDK / 嵌入模式被确认为硬需求 → 直接切 Rust（Rust 可导出稳定 C ABI，Go 不能）
- (T2) 团队 3 个月内无法补齐 Go 主力开发 → 评估切 Rust 或维持但降范围
- (T3) 硬抖动预算收紧到 GC 敏感量级（如绝对时间戳 P95 抖动 <1ms，为广播抖动 20ms 目标的 1/20）
- (T4) 上述 Rust 协议栈中任一（RTSP / enhanced-RTMP / WebRTC 全套 / MoQ）出现生产级可用实现，使"自研深度"重新回到可复用上游

**若采纳 Rust 的替代方案代价**（供决策对照）：M0–M2 范围不变的前提下，P0 需自研 RTSP + enhanced-RTMP + MoQ + WebRTC 四栈，估算新增 80–130 人日，M1 交付时间向后推 1.5–2.5 月；性能目标不变；内核设计、Unit/Path 抽象、插件契约、性能验收口径、集群路线**全部不变**——即 Rust 只影响"用什么写"，不影响"怎么写"。本设计的架构语言无关，这是把语言决策与架构决策解耦的有意设计，也是本条 6 个月后可无成本重评的前提。

### 7.2 单进程 vs 进程隔离
- **选项**：单进程全栈 / 每协议独立进程 / 内核 + 外部 gateway 进程（混合）
- **结论**：**单进程内核 + 受控 gateway 进程**（§5.4）。
- **放弃理由**：每协议独立进程会让广播变成跨进程复制，直接击穿"零拷贝"约束；反之全单进程无法承载受授权/闭源协议。
- **反悔成本**：中。gateway 契约已定义，可从"无 gateway"演进到"多 gateway"，不需改内核。

### 7.3 有状态 vs 无状态
- **选项**：无状态（纯代理，内容不落内核）/ 有状态（内核持 Stream）/ 分层（核心有状态 + 边缘无状态）
- **结论**：**内核有状态**（必须持有 Stream 与订阅图），边缘层无状态（read replica 不保存内容）。
- **放弃理由**：纯无状态代理意味着每对 reader 各拉一路 publisher，带宽与 CPU 都不可接受；这也是"多写者"必须禁止的原因。
- **反悔成本**：中。状态模型一旦确立，集群能力受限于 S1/S2 划分（§3.4），不能事后改变。

### 7.4 单端口复用 vs 多端口
- **选项**：每协议独立端口（MediaMTX 默认）/ 单端口复用（ZLMediaKit）
- **结论**：**默认多端口（部署简单、调试直观）+ 可选单端口复用模式**。
- **放弃理由**：单端口复用的首包解析风险（错误包放大、协议探测被滥用）与部分中间盒兼容性（NAT/防火墙对流特征的限制）在 v1 不应作为默认。
- **反悔成本**：低。两模式共存，L1 的 multiplexer 是可开关的。
- **偏离基线说明**：调研报告把"单端口复用 + 连接迁移"列为 QuickMedia 差异点；本设计保留其实现与能力，但不设为默认，避免首版稳定性风险。这是与调研建议 §6.2 第 4 条的**有意偏离**。

### 7.5 自研深度
- **选项**：全自研（ZLMediaKit 式）/ 全复用上游 / 分级（协议复用上游，差异点自研）
- **结论（v0.3 更新）**：**分级 + 内核自研**。
  - **协议栈（L1 传输 + L3 协议适配器）继续复用上游**：RTSP / RTMP / SRT / HLS / WebRTC / MoQ 复用 bluenviron/*、pion、moq。协议语义随标准演进，复用上游是"同一量级性能"目标的正确投入方向，也不改变内核抽象。
  - **内核三层 100% 自研（业主拍板）**：**L0** 内存与调度（Unit 引用计数、size-class 池、无锁 ring、UDPMux）、**L2** 容器与 codec 打包（ES/PS/TS/FLV/fMP4/KLV + Codec Registry）、**L5** 会话与流核心（PathManager、Path 状态机、Stream/SubStream、UnitRemuxer、广播与背压、绝对时间戳、GOP 缓存、订阅表）。
  - **差异点自研不变**：单端口复用、连接迁移、TS 透传、绝对时间戳、集群迁移、进程外网关契约。
- **放弃理由**：全自研包括协议栈会把 M1 推到不可控范围（P0 的 11 项协议语义细节是长期维护负担）；全复用上游会把内存池、容器格式与流会话状态交给外部包，直接摧毁"内核纯净"（G5）与零拷贝插桩的可审计性。分级自研让"内核可审计、协议可复用"同时成立。
- **内核自研新增的成本**：L2 容器编解码（TS/FLV/fMP4 的打包与解包）与 L0 内存池需要自建测试与 fuzz 投入，估算新增 15–25 人日，摊入 M0–M1；不改变任何对外性能目标。
- **反悔成本**：低-中。上游锁定通过 `go.mod` 版本锁 + 定期 rebase 管理，风险是上游 API 漂移（需为每个上游库写适配隔离层）。内核自研部分反悔成本为 0（本来就是自己的代码）。

### 7.6 是否内建转码
- **选项**：v1 内建 / v1 外部 worker + v2 内建 / 永远不内建
- **结论（v0.3 更新，业主拍板）**：**内建**。M2 交付内建转码首版，M3+ 扩展到 GPU 后端矩阵。原"v1 外部 worker、v2 视需求"的保守方案被覆盖；`TranscoderBackend` SPI 保留并作为**唯一**的转码接入点。
- **落地形态**：
  ```
  TranscoderBackend SPI（L4 Capability 层，唯一接入点）
     ├── TranscodeCapability    —— L4 Capability：订阅源 Unit → 后端 → 订阅者 Unit
     ├── builtin 后端            —— NVENC > QSV > VAAPI > libx264，按 GPU 可用性降级
     └── ffmpeg 后端             —— 进程外 FFmpeg worker，保留为可替换后端（降级或高吞吐场景）
  ```
- **隔离边界（内建后仍然成立，写进代码评审清单）**：
  1. 转码是 **Capability（L4）**，不是协议出口；它**不进入** publisher→reader 的直通路径，除非显式订阅。零拷贝约束（§4.2）仅约束直通路径，转码路径按 `memcpy ≤ 1（进编码器）+ 1（出编码器）` 独立计量并单独上报指标，不与直通路径的 0 拷贝目标混算。
  2. **后端在 worker goroutine 池内运行，受独立并发配额限制**；配额耗尽时返回显式 `ErrTranscoderBusy`，不阻塞媒体面广播。
  3. **GPU 资源由能力层封装持有**：显存池、会话并发上限、编码器实例生命周期、驱动崩溃恢复与重建全部在 capability 内部，L5 与 L2 不可见。内核仍然不出现任何编解码厂商名（G5 grep 门禁覆盖 `nvenc|qsv|vaapi|ffmpeg` 在 kernel/ 目录的禁止）。
  4. 转码失败的降级策略是**显式拒绝并上报**，不静默切换为直通（避免客户端拿到与请求不符的码流）。
- **为什么 v0.2 曾保守**：内建转码要把 GPU 生命周期、显存池、驱动崩溃恢复拉进内核热路径，且这是两家基线共同缺口、ZLMediaKit 闭源版的收费点。上述 4 条隔离边界是"内建而不污染内核"的具体机制——这也是把转码做成 Capability 而非改内核结构的原因。
- **反悔成本**：**低**。SPI 已定义，内建/外部后端可互换，未来若回退到纯外部方案，删除 builtin 后端即可，不影响内核与协议栈。
- **需同步的外部表述**：内建转码是本项目相对两家开源基线的差异化能力，可对外宣称；但"任意码流浏览器可播"仍取决于 §9.5 的浏览器兼容性约束，两者不可混为一谈。

### 7.7 数据面货币：Unit vs Frame vs GOP
- **选项**：GOP 粒度 / Unit 粒度 / RTP 包粒度
- **结论**：**Unit 粒度**（见 §2.2 论证）。
- **放弃理由**：GOP 粒度使慢消费者延迟代价过大；RTP 包粒度使 remux 与解码耦合，CPU 不可控。
- **反悔成本**：高（数据结构是内核地基），需在评审阶段锁定。

---

## 8. 里程碑与验收标准

每个里程碑的验收 = 功能清单 + **可执行证据**（基线数据 / 实测工时 / 自动化检查），不接受"已完成"式声明。

| 阶段 | 交付边界 | 验收证据（硬门槛） |
|---|---|---|
| **M0**（1 周，= MVP 阶段 1） | 骨架：L0/L2/L5 三层自研代码落位；跑通 RTSP→RTMP→HLS→WebRTC 全链路；确立 Unit/Track/Stream 数据结构与注册表接口 | ① 四协议端到端视频可播；② `grep` 门禁脚本可在 CI 通过；③ 接口文档评审通过（本文 §2.2 冻结）；④ **上游库可行性复核**（1 天，非阻塞）：核对 gortsplib / gortmplib / gohlslib / pion / moq / gosrt 的活跃度与许可，结论写入 §7.5 附注；任一 P0 协议上游不可用则改自研并评估工时 |
| **M1**（1–2 月，= MVP 阶段 2） | P0 协议 7 项（RTSP/RTMP/HTTP-FLV/HLS/WebRTC/SRT/TS+RTP）+ Control API + Hooks + Prometheus | ① 性能基线 7 项实测数据（§4.5）全部产出并入库；② 单核 ≥1 万路 RTSP 只读（未达标则给出差距分析与是否降级目标）；③ 每 Unit 拷贝 = 0 的插桩证据；④ `bench` 子命令可复现 |
| **M2**（2 月，= MVP 阶段 3） | + MoQ、fMP4 录像、绝对时间戳、always-available、点播+seek、JWT、TS 透传、**内建转码首版**、**内建 SIP 核心栈** | ① WebRTC 单跳 P95 < 300ms、LL-HLS < 1.5s 实测；② 录像回放与直播无缝切换演示；③ **易扩展演练**：新增一个玩具协议 + 1 个 codec + 1 个 capability，实测工时并写入本文 §5.1（内核 diff = 0 行）；④ 内建转码：H.264 跨分辨率转码 + AAC 采样率转换可用，CPU 与 GPU 后端均跑通，吞吐基线入库；⑤ SIP：RFC 3261 注册 / INVITE / BYE 与外部 SIP 测试床（如 SIPp）互通 |
| **M3**（3 月） | + GB28181（含对讲 + 主动拉流 + 级联）、ONVIF、单端口复用 + 连接迁移、HTTP-TS/fMP4 | ① 与市面主流 NVR/平台对接联调报告（≥3 款设备）；② 连接迁移在断网重连场景的恢复时间实测（目标 ≤3s）；③ E2 拓扑（read replica）跑通并给出溯源带宽数据 |
| **M4**（3–6 月） | + 集群（E3 会话迁移）、S3 录像直写、进程外 gateway 契约落地（NDI 或国标 C 网关其一）、SDK/文档 | ① 故障转移演练：source 节点 kill → reader 恢复 ≤3s（P95）；② ≥1 个第三方协议以 gateway 形态接入且内核 diff = 0；③ 集群模式下总并发 ≥ 单机 3 倍（给出拓扑与瓶颈分析） |

**两条硬约束的量化验收口径（总括）：**

- **高性能**：M1 交付基线数据；此后任一版本相对基线退化 >10% 必须解释；对外宣称的数字必须可追溯到测试脚本 commit。
- **易扩展**：M2 实测"新增协议 ≤3 人日 / 新增 codec ≤1 人日 / 新增能力 ≤2 人日"，且内核 diff = 0 行；CI grep 门禁长期有效。
- **集群**：E3 的验收是"故障转移恢复 ≤3s（P95）"，不是"无感切换"（§5.3）。

---

## 9. 风险与需要拍板的开放问题

### 9.1 【已拍板 v0.3】GB28181 的 SIP 栈选型 —— 内建，自研
**结论**：走 (a) **自研 SIP 栈**，C 网关（opensips/kamailio）降级为可选兼容路径而非默认。
- **实施范围**：SIP 核心栈（RFC 3261 事务与对话框、RFC 3262 重传与可靠性、REGISTER / INVITE / ACK / BYE / CANCEL / REFER + SDP）进 **M2**；GB/T 28181 扩展（PS 解封装、双向对讲、主动拉流、级联目录、设备能力集协商）进 **M3**。
- **工程评估**：SIP 核心可复用 pion 的 UDP/TCP 传输与 SDP 库，自研的是 SIP 语义层，估 **15–20 人日**；GB28181 对接侧估 30–40 人日，主要是标准细节与设备差异，不是语言或栈能力问题。
- **GB28181 仍保留 P1，不进入 P0**（原判断不变）：P0 首版不依赖国标即可交付可用产品。
- **风险与缓解**：SIP 互通的真实风险不在协议实现而在设备差异（NAT 穿越、SDP 字段容忍度、级联时序）。缓解：M2 用 SIPp 等公开测试床做互通验证，M3 再对真设备联调 ≥3 款；若 M2 验证失败则回落到 C 网关路径（网关契约 §5.4 已定义，内核 diff = 0）。

### 9.2 【已拍板 v0.3】内建转码 —— 作为承诺能力
**结论**：内建转码由"v2 可选项"升级为 **v1.x 承诺能力**，M2 交付首版。隔离机制与落地形态见 §7.6，性能口径见 §8。
- **承诺边界（可对外表述，超出即不可兑现）**：承诺的是"内建转码"这一能力——同编码家族内的分辨率/码率/帧率调整、音视频采样率转换、H.264↔H.265 转封装级转换。不承诺：任意编码族互转的画质保真、超分/HDR 转换、浏览器兼容兜底（后者由 §9.5 的显式拒绝策略处理）。
- **投入**：首版（libx264 + NVENC 两后端）估 **20–25 人日**，摊入 M2；GPU 后端矩阵（QSV/VAAPI）与稳定性硬化进 M3。
- **代价已接受**：这是两家开源基线共同缺口、ZLMediaKit 闭源版的收费点，意味着长期承担 GPU 驱动与编码参数矩阵的维护投入。换来的是相对 ZLMediaKit / MediaMTX 的差异化卖点。

### 9.3 【已拍板 v0.3】内核重写 —— 内核三层自研，协议栈仍复用上游
**结论**：可行，已执行。"重写内核"落实为 **L0 / L2 / L5 三层 100% 自研**，不引入第三方媒体内核或媒体面库：
- **L0 内存与调度**：Unit 引用计数、size-class 对象池、无锁 ring、UDPMux（单读循环 + GRO + 批量读）
- **L2 容器与 codec**：ES/PS/MPEG-TS/FLV/fMP4-CMAF/KLV 编打包 + Codec Registry + 协商矩阵
- **L5 会话与流核心**：PathManager、Path 状态机、Stream/SubStream、UnitRemuxer、广播与背压、绝对时间戳（NTP↔RTP 锚点）、GOP 缓存、订阅表

**边界说明（有意保留，非遗漏）**：L1 传输与 L3 协议适配器继续复用上游库（gortsplib / gortmplib / gohlslib / pion / gosrt / moq）。理由：协议语义随标准演进，是长期维护负担而非差异化；"内核"在本设计中的定义是承载媒体面抽象与状态的层，协议栈不属于内核（见 §7.5）。若未来需要全栈自研，架构无需改动，只影响工时估算。

**C SDK / 嵌入模式仍非目标**：Go 无法提供稳定 C ABI，故 v1 不做嵌入模式（§1.3）。因此 §7.1.1 的重启条件 **T1 不触发**，语言决策维持 Go。若日后确认为硬需求，直接切 Rust 重做内核，届时语言决策与 C SDK 需求同解。

**语言决策的重启条件（§7.1.1，无需重新审批即可重启）**：T1 C SDK 为硬需求（当前已确认非硬需求）；T2 团队 3 个月内无法补齐 Go 主力；T3 硬抖动预算收紧到 GC 敏感量级；T4 Rust 协议栈出现生产级实现。

### 9.4 上游依赖漂移
bluenviron/* 迭代快。缓解：为每个上游库写适配隔离层（L3 只依赖本项目的 Adapter 接口）；`go.mod` 版本锁 + 每月 rebase 演练。**风险等级：中，可控。**

### 9.5 浏览器兼容性
WebRTC 出口的 H.265 / H.264-B-frames 是硬伤（调研报告 §7.3）。本设计的选择是"显式拒绝 + 建议"，而非静默失败或自动转码。若产品要求"任何码流都能在浏览器播放"，则 §9.2 的转码决策会连带变化。

### 9.6 性能宣称的风险
在没有 M1 基线数据前，本设计中的所有数字均为**目标值**。禁止在任何对外材料中以实测口径使用。

### 9.7 "所有多媒体协议"的表述风险
P0+P1+P2 覆盖后仍有明确不做的项（NDI/ISUP 授权、SFU/MCU 会议、逐协议全内建）。建议对外表述为"**主流多媒体协议全支持 + 受授权/专有协议以网关方式接入**"，避免不可兑现的承诺。

---

## 10. 附录

### 10.1 建议目录结构

```
quickmedia/
├── kernel/                  # L5 会话与流核心（禁止协议名，grep 门禁）
│   ├── stream/              # Unit/Track/Stream/SubStream/UnitRemuxer
│   ├── path/                # Path 状态机、PathManager、订阅表、背压
│   ├── clock/               # 绝对时间戳 / NTP↔RTP 锚点
│   └── registry/            # Module 注册表、协商矩阵
├── transport/               # L1 TCP/UDP/QUIC/TLS + 单端口复用 + 连接迁移 + UDPMux
├── container/               # L2 ES/PS/TS/FLV/fMP4 + Codec 接口与内置 codec
├── adapters/                # L3 协议适配器（每协议一个包，均实现 Adapter）
│   ├── rtsp/ rtmp/ httpp/ hls/ webrtc/ srt/ mpegts/ rtp/ moq/
│   ├── gateway/             # 进程外网关客户端（gRPC + 共享内存 ring）
│   └── sip/                 # 内建 SIP 栈（RFC 3261/3262 + GB/T 28181 扩展）
├── capabilities/            # L4 录像/回放/截图/转推/转码代理/存储
│   ├── recorder/ playback/ snapshot/ forwarder/ storage/
│   └── transcoder/            # 内建转码 Capability + TranscoderBackend SPI（builtin / ffmpeg 后端）
├── control/                 # L6 OpenAPI 定义 + 生成 + handler + auth + hooks
├── memory/                  # L0 对象池、引用计数、共享内存 ring
├── bench/                   # 性能基线与回归
└── cmd/quickmedia/          # 单二进制入口
```

### 10.2 与两个参考基线的偏离清单（汇总）

| 偏离 | 基线做法 | 本设计 | 理由 |
|---|---|---|---|
| 单端口复用设为可选而非默认 | ZLMediaKit 默认单端口 | 默认多端口 + 可选复用 | 首包解析风险与中间盒兼容性，见 §7.4 |
| 转码外部化 | 两家开源版均外部化；闭源版内建 | v1 外部 worker + SPI 预留 | §7.6 |
| 单发布者 + 明确拒绝混流 | 两家均单发布 | 同，且 API 层显式 409 | 保证时间戳/关键帧语义一致 |
| 内核零协议字段 + CI grep 门禁 | 两家均有单文件巨型化（WebApi 119KB、core 46KB） | 模块化 + 依赖方向门禁 | 针对可维护性风险的显式对策 |
| 集群以"快速恢复"替代"透明迁移" | ZLM 溯源集群 / MediaMTX read replicas | E1→E2→E3，恢复 ≤3s | S2 状态不可迁移（§3.4） |
| TS 透传进入 P1 | 两家开源版均无 | 内建 `PassthroughMode` | 广电/多路分发省 CPU，工程可行性明确 |
| 进程外网关契约 | 两家均无统一跨进程数据面契约 | gRPC + 共享内存 ring | 覆盖受授权/闭源协议且不污染内核 |
| 内建转码 | 两家开源版均外部化；ZLMediaKit 闭源版内建并收费 | **内建**（Capability + 后端 SPI，M2 首版） | 业主拍板；隔离边界见 §7.6，是本项目的差异化卖点 |
| 内建 SIP | 两家均无 SIP 实现 | **内建自研**（pion 复用传输/SDP） | 业主拍板；C 网关降级为可选兼容路径 |
| 内核自研 | ZLMediaKit 全自研；MediaMTX 复用上游 | **L0/L2/L5 自研 + 协议栈复用上游** | 内核可审计（G5 + 插桩证据），协议语义不背维护包袱 |

### 10.3 前置依赖（不阻塞 M0）

- M0 前需确认 §9.3（C SDK 是否为硬需求）。若需要，M0 的接口冻结要按 C ABI 重新设计。
- M1 前需完成 §4.5 的测试脚本与硬件基线环境（一台标准 x86 实例即可，先不追求多规格）。
- M2 前需完成 §9.1 的 GB28181 spike，否则 M3 范围应下调（把 GB28181 移到 M4）。
