# QuickMedia 需求调研报告：ZLMediaKit 与 MediaMTX 对照，与"全多媒体协议"的真实边界

调研日期：2026-09-19
调研范围：ZLMediaKit (`github.com/ZLMediaKit/ZLMediaKit`, master)、MediaMTX (`github.com/bluenviron/mediamtx`, main)。证据来自两仓库 `README.md` 功能清单、源码目录树（目录级枚举）与 `docs/` 官方文档。所有事实均标注了来源。

---

## 0. 结论先行

1. **"支持市面上所有多媒体协议"在工程上不存在**。两家加起来覆盖约 20 类协议 + 20+ 编解码，但都缺 **国标安防（GB28181 / ONVIF / JT1078 / SVAC / ISUP）**（ZL 部分覆盖）、**NDC/NDI**、**MPEG-DASH/CMAF 点播**、**MPEG-2 TS 透传直出**、**内建转码**（两家都是"外部 FFmpeg/GStreamer"或闭源）。QuickMedia 要立的不是"再做一个 ZL"，而是"把两家缺的部分补上 + 把 MOQ 当一等公民"。
2. **两者是两种截然不同的架构哲学**，QuickMedia 必须先选一个作为地基，而不是同时做：
   - ZLMediaKit = **Frame/Track 归一化 + 每协议四件套（Session/Demuxer/Muxer/Player/Pusher）**，可嵌入成 SDK，强在**性能与安防场景**，弱在 MoQ/绝对时间戳/转码。
   - MediaMTX = **Path/Stream + unit_remuxer + 外部协议库（gortsplib / gortmplib / gohlslib / gosrt / pion / moq）**，强在**工程完备度、MoQ、可观测性、零依赖单二进制**，弱在国标、无内建转码。
3. **推荐的落地路径**：v1 走 MediaMTX 路线（Go + 复用 bluenviron 系列库 + pion），把 GB28181 / ONVIF / 转码 / 集群作为 v1.5–v2 补齐。理由见第 6 节。
4. QuickMedia 的差异化抓手（也是"高性能"真正能被验证的口径）：**单端口 TCP/UDP/QUIC 复用 + 连接迁移**（ZL 唯一）、**全栈绝对时间戳**（MediaMTX 已有）、**MOQ + LL-HLS + WebRTC 三低延迟出口**、**GB28181 双向对讲 + 国标透传**。

---

## 1. 项目基线对照

| 维度 | ZLMediaKit | MediaMTX |
|---|---|---|
| 语言 / 版本基线 | C++11 | Go |
| 许可证 | MIT（GitHub repo LICENSE） | MIT |
| 星标 | 约 18k（shields.json） | 约 20k（shields.json） |
| 交付形态 | 独立二进制 + **C SDK（`api/` → `mk_*.h`）+ 多语言绑定** | **零依赖单二进制**（README） |
| 官方定位 | "运营级流媒体服务框架"，**可嵌入** | "ready-to-use live media server / media proxy"，**可运行** |
| 平台 | linux / macos / ios / android / windows；x86 / arm / risc-v / mips / 龙芯 / 申威 | linux / windows / macos |
| 自研深度 | ICE/STUN/DTLS/SRTP/SDP/SCTP **全部手写**（`webrtc/` 下 IceTransport.cpp 81KB、Sdp.cpp 77KB、DtlsTransport.cpp 45KB） | 依赖 **pion / gortsplib / gortmplib / gohlslib / gosrt / moq / mediacommon**（目录名可见） |
| 内嵌第三方 | `3rdpart/` + `ext-codec/` + 内嵌 `pugixml`（`src/Onvif/pugixml.cpp` 337KB） | 无 C 依赖，仅 Go modules |
| 转码 | 开源版**无**；闭源专业版有（配置/API/软硬自适应/按需/滤镜/全 GPU） | **无**，明确建议外部 FFmpeg / GStreamer（docs/2-features/07） |

来源：ZL `README.md`（"项目特点/功能一览/闭源专业版"）、MediaMTX `README.md`、两仓库目录树。

---

## 2. 协议 / 编解码覆盖对照

### 2.1 传输与信令层

| 协议 | ZLMediaKit | MediaMTX |
|---|---|---|
| RTSP / RTSPS（UDP、UDP 组播、TCP 三种 transport） | ✅（README：rtp over udp / tcp / http / 组播四种） | ✅（含 TCP 内嵌 RTSP、RTSP over HTTP / WebSocket 自动识别、`rtsp+http` / `rtsps+http` / `rtsp+ws` 客户端 scheme） |
| RTSP 加密（RTSPS / SRTP / SRTCP） | ✅ | ✅（`rtspEncryption: optional`，docs/2-features/26） |
| RTSP MPEG-TS 载荷 | ✅（`es/ps/ts/ehome rtp`） | ✅（`internal/protocols/rtsp/mpegts_demuxer.go`；docs/3-publish/07 标题即 "MPEG-TS inside RTSP"） |
| RTMP / RTMPS / HTTP-FLV / WS-FLV | ✅（含 RTMP-H265、RTMP-OPUS、**enhanced-RTMP**） | ✅ |
| HLS（mpegts / fMP4 两种 variant） | ✅（含 LL-HLS，fmp4 variant） | ✅（`hlsVariant: fmp4`，含 LL-HLS） |
| HTTP-TS / WS-TS | ✅ | — |
| HTTP-fMP4 / WS-fMP4 | ✅ | — |
| WebRTC（含 WHIP / WHEP） | ✅ 全自研，**ICE-full**，可作客户端拉/推/P2P，**单端口 + 连接迁移**，TWCC / NACK / RTX / datachannel / RTC-over-TCP | ✅（基于 pion；WHIP RFC9725、WHEP draft-ietf-wish-whep） |
| SRT | ✅（`srt/` 自研，含加密、NAK、`SrtCaller`） | ✅（gosrt） |
| MPEG-TS over UDP / multicast / Unix socket | ✅ | ✅（`udp+mpegts://`、`unix+mpegts://`；Unix socket 比 UDP 更高效） |
| Raw RTP（UDP，带 SDP 描述） | ✅ | ✅（`udp+rtp://` + `rtpSDP`） |
| **Media-over-QUIC（MoQ，RFC9000/9114 + draft-moq-transport/MSF/LOC）** | ❌ | ✅（`internal/protocols/moq`：catalog / controlmessage / namespace / parameter / property / reorderer / subgroup / varint；transport 可选 `quic` / `webtransport`） |
| RTP over HTTP / 组播 | ✅ | ❌ |
| GB/T 28181（SIP + PS + RTP） | ✅（含**主动拉流**、**双向语音对讲**；`src/Rtp/GB28181Process.cpp`） | ❌ |
| ONVIF / PSIA | ✅（`src/Onvif`，内嵌 pugixml 的 SOAP） | ❌ |
| JT/T 1078 | 仅闭源版（透传/级联/对讲） | ❌ |
| 点播 VOD + seek | ✅（RTSP/RTMP/HTTP-FLV/WS-FLV 支持 MP4 点播 + seek） | ✅（`internal/playback` + `internal/recordstore` + `api/openapi.yaml` 里 41KB 的 playback spec） |
| 录像 | ✅ FLV / HLS / MP4；闭源版 S3/MinIO 直写 + 云存储网页点播 | ✅ **fMP4 / MPEG-TS 自研容器 muxer**（`internal/recorder`）+ `recordcleaner` |
| "先播后推" / 断连补画面 | ✅ "先播放后推流" | ✅ **always-available**（用离线段无缝拼接，无重编码） |
| 集群 / 水平扩展 | ✅ **溯源集群**（溯源支持 rtsp/rtmp/hls/http-ts，多源站 round-robin，边沿站 hls） | ✅ **read replicas**（L4 LB 用于 RTSP/RTMP/SRT，L7 + sticky 用于 HLS/WebRTC）+ **CDN 前置**（HLS + `hlsCDNSecret` Bearer 鉴权） |
| 代理 / 转推 | ✅ 按需转协议、按需推/拉流、按需解复用 | ✅ **proxy**（`paths.<name>.source`，任意协议入，含 `sourceOnDemand`）+ **forward**（MoQ / SRT / WebRTC-WHIP / RTSP / RTMP + FFmpeg；含 YouTube / Twitch 指南） |
| 绝对时间戳 | 弱（未见全链路透传） | ✅ **全栈**：HLS `EXT-X-PROGRAM-DATE-TIME`、RTSP/WebRTC RTCP Sender Report；`useAbsoluteTimestamp`；NTP ↔ RTP 换算公式见 docs/2-features/15 |
| 多轨道（同一流多视频/音频） | ✅ 全部流协议 | ✅（`internal/stream/sub_stream.go` + `sub_stream_format.go`，可按 track 分叉） |
| 控制面 | `WebApi.cpp` 119KB RESTful + `WebHook.cpp` 37KB + shell + Python invoker（`pyinvoker.cpp` 31KB） | Control API（`api/openapi.yaml` 132KB，OpenAPI 生成文档）+ **Hooks**（外部命令）+ **JWT** 鉴权 |
| 可观测 | 流量统计 + HTTP API | **Prometheus metrics + pprof**（heap / cpu / goroutine） |
| 配置热加载 | ✅（含 SSL 证书热加载） | ✅（`internal/confwatcher`，fsnotify） |
| 鉴权 | RTSP Basic/Digest 全异步可配置；HTTP 文件鉴权 | internal / HTTP / **JWT**（docs/2-features/06，15KB） |

来源：ZL `README.md` 功能清单逐行、ZL `conf/readme.md`、`webrtc/USAGE.md`、`srt/srt.md`；MediaMTX `README.md`、`docs/2-features/*`、`docs/3-publish/*`、`docs/6-misc/4-specifications.md`、`mediamtx.yml`。

### 2.2 编解码层

| 编解码 | ZLMediaKit | MediaMTX |
|---|---|---|
| 视频 | 全支持 H.264 / H.265 / AV1 / VP8 / VP9；部分 JPEG / **H.266 (VVC)** / SVAC / MP2V | H.264 / H.265 / AV1 / VP8 / VP9 / MPEG-4 Video (H.263 / Xvid) / MPEG-1/2 Video / M-JPEG |
| 音频 | 全支持 AAC / G.711 / Opus / MP3 / MP2 / G.722 / G.723 / G.729 / ADPCM / LPCM | Opus / AAC / MP3 / AC-3 / G.726 / G.722 / G.711 / LPCM |
| 其他 | SVAC | **KLV**（SDI 时间码，docs/3-publish/12、13） |
| 多轨道 | ✅ | ✅ |

ZL 的编解码支持矩阵在 `ext-codec/` 目录可以逐文件核对（`H264Rtp.cpp`、`H265Rtp.cpp`、`AV1Rtp.cpp`、`JPEGRtp.cpp`、`VpxRtmp.cpp`、`MP2ARtp.cpp`、`MP3Rtp.cpp` … 每 codec 一对 `*Rtp` + `*Rtmp` 封装实现）。

来源：ZL `README.md` "全协议支持 H264/H265/…" 行、`ext-codec/` 目录；MediaMTX `docs/3-publish/12-mpeg-ts.md` 与 `13-rtp.md` 的 codec 表格。

### 2.3 "所有多媒体协议"缺什么

两家都未覆盖，且这是 QuickMedia 可以明确宣称差异的地方：

| 缺口 | 场景 | 备注 |
|---|---|---|
| NDI / 3G-SDI-over-UDP | 演播室 / 广电 | 专有协议，需授权 |
| MPEG-DASH / CMAF 点播 | 长视频点播 | 两家都只做直播 |
| MPEG-2 TS **透传直出**（不解封装） | 广电 / 多路分发省 CPU | ZL 开源版需解复用；闭源版有 TS 透传 |
| 内建转码 / 转封装 / 缩放 / 滤镜 | 兼容异构码流 | ZL 闭源版有；MediaMTX 靠外部 |
| ISUP / 10151-30113 / GB/T 20980 | 国内广电 | 极少实现 |
| WebRTC SFU / MCU 会议 | 互动会议 | ZL 闭源版有 MCU；两家开源版都是 P2P/单向 |
| 边缘渲染 / 360 / HDR (H.265 HDR10) | 沉浸式 | 均需额外元数据管道 |

---

## 3. 架构对照（源码结构级）

### 3.1 ZLMediaKit：`Frame/Track → MediaSource` 归一化模型

```
src/
├── Extension/     Factory / Frame / Track / CommonRtp / CommonRtmp     ← 归一化核心
├── Codec/         Transcode (30KB) / H264Encoder / AACEncoder          ← 转码在核心
├── Common/        MediaSource / MediaSink / MultiMediaSourceMuxer /
│                  config / PacketCache / Stamp / Parser                 ← 广播 + 配置
├── Rtsp/          RtspSession / RtspPlayer / RtspPusher /
│                  RtspMediaSource[Imp|Muxer] / RtspMuxer / RtspDemuxer / RtpMultiCaster
├── Rtmp/          同构四件套 + amf.cpp (17KB)
├── Rtp/           RtpSender / RtpServer / GB28181Process / PS|TS|Raw Encoder|Decoder
├── Http/          HttpSession / HlsPlayer / TsPlayer / FMP4 / HttpClient / WebSocket
├── TS/ FMP4/      TSMediaSource(Muxer) / FMP4MediaSource(Muxer)         ← 薄壳，复用 Common
├── Onvif/         Onvif + SoapUtil + 内嵌 pugixml                       ← 内嵌第三方
└── Record/        HlsMaker / MP4Muxer|Demuxer|Reader / MPEG / Recorder

webrtc/            自研 ICE / STUN / DTLS / SRTP / SDP / SCTP / NACK / TWCC / RtpExt
srt/               自研 SRT（Packet / PacketQueue / Crypto / HSExt / Ack）
api/               mk_*.h C SDK（player/pusher/proxyplayer/recorder/rtp_server/
                   webrtc/transcode/events/httpclient/tcp/thread）      ← 可嵌入
server/            WebApi / WebHook / ShellParser / FFmpegSource / Process /
                   VideoStack / pyinvoker
ext-codec/         每 codec 的 *Rtp / *Rtmp 封装                          ← 编解码矩阵
tests/             test_bench_{push,pull,forward,proxy}.cpp + 30+ 单元   ← 有 benchmark 基线
```

**核心设计判定**：一切协议进出都过 `Frame`（codec unit）+ `Track`（codec + 参数）；每协议实现"四件套"（Session 握手 / Demuxer 解封 / Muxer 封装 / Player·Pusher 推拉）。`MediaSource`（34KB）负责订阅/广播，`MultiMediaSourceMuxer`（34KB）负责把 N 个源扇出到 M 个协议。这是"能同时上 20 类协议"的根本原因。

代价：`WebApi.cpp` 119KB、`webrtc/Sdp.cpp` 77KB、`IceTransport.cpp` 81KB、`DtlsTransport.cpp` 45KB、`MediaSource.cpp` 34KB —— **单文件巨型化**是主要可维护性风险。

### 3.2 MediaMTX：`Path → Stream → readers` 广播模型

```
internal/
├── core/          core.go (46KB) / path.go (31KB) / path_manager.go (20KB)
├── protocols/     hls / httpp / httpp3 / moq / mpegts / proxy / rtmp /
│                  rtsp / tls / udp / unix / webrtc / websocket / whip
├── servers/       hls / moq / rtmp / rtsp / srt / webrtc
├── stream/        stream.go + unit_remuxer.go + sub_stream*.go +
│                  rtp_encoder.go / rtp_decoder.go / offline_sub_stream*
├── recorder/      format_fmp4.go (21KB) + format_mpegts.go (10KB)
├── recordstore/   recordcleaner/                            ← 录像生命周期
├── playback/                      internal/playback          ← 录像回放
├── forward/       forward_dest_info.go                     ← 主动转推
├── staticsources/ 录制/文件源                                 ← 静态源
├── api/           30+ 个 api_*.go，与 OpenAPI 一一对应，每个都配 *_test.go
├── auth/ conf/ confwatcher/ metrics/ pprof/ logger/
├── hooks/ externalcmd/ rlimit/ restrictnetwork/
├── ntpestimator/ packetdumper/ counterdumper/ upgrade/
└── test/ teste2e/ unit/
```

**核心设计判定**：单发布、单 stream、扇出到任意 reader；**关键抽象是 `unit_remuxer.go`** —— 在 unit（GOP 内最小可重打包单元）粒度做重打包，这让"不重编码就能换容器/换协议"成为可能，也让 `sub_stream.go` 能按 track 分叉出"子流"。协议层一律写成薄适配（`internal/protocols/<x>/{from_stream.go,to_stream.go}`），把脏活交给 `bluenviron/*` 库。

代价：核心逻辑集中在 `core.go` 46KB + `path.go` 31KB + `api_test.go` 51KB —— 同样有单文件巨型化，但 Go 的测试覆盖（每个 api 都有对应 `*_test.go`，`internal/core/core_test.go` 4.2KB、`path_test.go` 29KB）显著降低了风险。

### 3.3 两家的共同取舍（这是行业共识，不是巧合）

1. **不做内建转码**，把编解码格式变更交给外部 FFmpeg / GStreamer 进程。
2. **不做 WebRTC SFU/MCU**，只做一对多单向广播。
3. **单发布模型**：每个 path/stream 只允许一个 publisher。
4. **HTTP 作为统一控制面 + 网关隧道**（RTSP-over-HTTP / WHIP / WHEP / API 全走 HTTP）。
5. **录像走自研容器 muxer**（fMP4 + MPEG-TS），不依赖 GStreamer muxer。

这些是 QuickMedia 应当**沿用**的边界，而不是重复造轮子的地方。

---

## 4. "超高性能"的真实测量口径

"高性能"在媒体服务器里只有三个可测维度，两家给出的公开口径分别是：

| 维度 | 典型瓶颈 | ZL 的做法 | MediaMTX 的做法 |
|---|---|---|---|
| 单路 CPU | 拷贝、编解码 | 零拷贝 Frame + 按需解复用（`README`：无人观看不开启转协议） | `unit_remuxer` 在 unit 粒度只做必要重打包；`README` 定位 "without re-encoding" |
| 单核并发连接数 | 每连接 goroutine/线程 + epoll 开销 | **epoll + 多线程 + 单端口复用**（`README`：单端口、多线程、连接迁移，"开源界唯一"） | Go netpoll + 每连接 goroutine；`internal/rlimit` 调 fd；pprof 样例显示热点在 `syscall.recvfrom`（UDP multicast），非 GC |
| 扇出带宽 | 到 reader 的带宽，不是 CPU | 溯源集群 + 边沿站 HLS | **read replicas（L4/L7 分流）+ CDN 前置 HLS**；`docs/2-features/19-scalability.md` 给出完整 AWS/Traefik 拓扑 |

**关键洞察**：MediaMTX 的官方文档明确指出，在不重编码的场景下，**瓶颈几乎永远是 server ↔ reader 之间的带宽**，而不是 CPU。因此"高性能"的正确优化顺序是：
1. **降低每路开销**（零拷贝、按需转协议、按需拉流）；
2. **降低到 reader 的带宽**（HLS-fMP4 + CDN、组播、read replicas）；
3. **最后才是**多核/NUMA/DPDK。

QuickMedia 若声称"超高并发"，必须绑定这三个口径的具体数字（例如"单核 1 万路 RTSP 只读、20k GOP/s 转协议、100 万读者 HLS+CDN"），否则是营销词。

---

## 5. "简单易用"的构成

两家的"简单"是不同维度的：

| 维度 | ZLMediaKit | MediaMTX |
|---|---|---|
| 部署 | 单二进制 + Docker + k8s（`k8s_readme.md`） | **零依赖单二进制**，README 一句 `docker run bluenviron/mediamtx:1` |
| 配置 | `conf/config.ini`（42KB，参数极多） | `mediamtx.yml`（38KB）+ 环境变量 + 配置热加载 |
| 上手路径 | 需要理解 Frame/Track/MediaSource + shell + Python | `paths.<name>.source: rtsp://...` 一行即可 |
| 控制 | RESTful + WebHook + shell + **Python 内嵌**（`pyinvoker`） | RESTful Control API + **外部命令 Hooks** + JWT |
| 生态接入 | `ext-codec/` + C SDK 可嵌入任意应用 | 官方文档列出 20+ 种客户端接入方式（FFmpeg/GStreamer/OBS/VLC/Python OpenCV/Go/Unity/Raspberry Pi/浏览器） |
| 文档 | README 长功能清单 + wiki | 结构化 `docs/1-kickoff → 6-misc`，含 AWS 部署手把手 |

QuickMedia 若要"简单易用"，v1 必须给出：一条命令部署（Docker Compose 或裸二进制）、一份 100 行以内的默认配置、一个可视化路径列表（哪怕只是 `curl /api/v1/paths`）。

---

## 6. QuickMedia 架构建议

### 6.1 语言与地基选型

**推荐：Go 为主，`bluenviron/*` + `pion` + `moq` 库为协议引擎；转码走 FFmpeg 子进程；关键热路径保留 C/cgo 逃逸口。**

| 选项 | 优势 | 代价 | 判定 |
|---|---|---|---|
| **Go（MediaMTX 路线）** | 7+ 协议库生产验证；单二进制；OpenAPI/Prometheus/pprof 免费；测试与热加载成熟 | 极小 UDP 包路径（GB28181 组播）比 C++ 慢 ~1.5x；无法作为 C SDK 嵌入 | ✅ **v1 首选** |
| C++（ZLM 路线） | 峰值性能；可嵌入 SDK；与 FFmpeg/GStreamer 原生集成 | WebRTC 栈需自建（81KB ICE + 77KB SDP 起步的技术债）；无 MoQ 生态；迭代慢 | ❌ v1 不选，作为 v2 性能逃逸口 |
| Rust | 内存安全 + 性能 | 生态缺（无成熟 RTSP/RTMP/GB28181/MoQ） | ❌ |
| 混合：C++ 核心 + Go 控制面 | 兼顾 | 维护双栈成本翻倍，IPC 有开销 | ❌ |

**注意**：MediaMTX 的 pprof 示例输出（`docs/2-features/23-performance.md`）显示，在真实流量下热点集中在 `syscall.recvfrom`（UDP 组播读包）而非 GC，说明 Go 的选择在 UDP 密集场景不是致命项。

### 6.2 分层与核心抽象

```
┌──────────────────────────────────────────────────────────────┐
│  Control Plane (Go)                                           │
│  Control API (OpenAPI) · Hooks · JWT · Prometheus · pprof    │
│  CLI · Web UI (可选) · Config hot-reload                      │
├──────────────────────────────────────────────────────────────┤
│  Path Manager  —  鉴权 · 广播 · always-available · 子流       │
│  Path ── Stream ── [Track × N] ── Unit Remuxer               │
├──────────────────────────────────────────────────────────────┤
│  Protocol Adapters (薄适配, from_stream / to_stream)          │
│  RTSP·RTMP·HLS·WebRTC·SRT·MPEG-TS·RTP·MoQ·GB28181·ONVIF     │
├──────────────────────────────────────────────────────────────┤
│  Container / Framing  —  ES · PS · TS · FLV · fMP4/CMAF       │
├──────────────────────────────────────────────────────────────┤
│  Codecs  —  H.264/265 · AV1 · H.266 · VP8/9 · M-JPEG ·       │
│            AAC · Opus · G.7xx · MP3 · AC-3 · KLV              │
├──────────────────────────────────────────────────────────────┤
│  Transport  —  TCP · UDP (+ multicast) · QUIC/H3 · WebTransport │
│  单端口复用 · 连接迁移 · STUN/TURN/ICE (pion)                  │
└──────────────────────────────────────────────────────────────┘
```

**必须内建的能力（两家共同缺口，也是 QuickMedia 的护城河）：**

1. **内建转码/转封装引擎**（不是必选功能，但要留接口）：v1 用 FFmpeg subprocess + `runOnAvailable` hooks（MediaMTX 模式）；v2 提供 FFmpeg graph 进程内模式或 NvTranscode。
2. **国标安防**：GB28181（SIP+PS+RTP，含**双向对讲** + 主动拉流 + 级联）、ONVIF（WS/TS）、JT/T 1078。ZL 开源已验证 GB28181 与 ONVIF 可行。
3. **绝对时间戳全链路**：HLS `EXT-X-PROGRAM-DATE-TIME` + RTSP/WebRTC RTCP SR + NTP ↔ RTP 换算（MediaMTX docs/15 已给公式）。安防/广电回放场景刚需。
4. **单端口 TCP/UDP/QUIC 复用 + 连接迁移**：ZL 已验证（`README` 标注"开源界唯一"），对 k8s/NAT/云环境价值极大。
5. **TS 透传直出**：广电/多路分发省 CPU 的关键（ZL 闭源版有，开源版无）。
6. **录像云存储直写**（S3 / MinIO）+ 网页点播（ZL 闭源版有）。

### 6.3 协议支持矩阵（v1 目标）

| 协议 | v1 | v2 | 说明 |
|---|---|---|---|
| RTSP / RTSPS | ✅ | | UDP/TCP/multicast + TLS + MPEG-TS 载荷 |
| RTMP / RTMPS / enhanced-RTMP | ✅ | | H.265 / VP8/9 / AV1 / Opus via enhanced-RTMP |
| HTTP-FLV / WS-FLV | ✅ | | 浏览器兼容兜底 |
| HLS (mpegts + fMP4, LL-HLS) | ✅ | | 含 CDN 前置 + `hlsCDNSecret` |
| WebRTC (WHIP/WHEP) | ✅ | | pion；TWCC / NACK / RTX |
| SRT | ✅ | | 加密 + NAK |
| MPEG-TS (UDP/multicast/Unix socket) | ✅ | | Unix socket 更高效 |
| Raw RTP (UDP, SDP) | ✅ | | |
| **Media-over-QUIC (MoQ)** | ✅ | | **差异化抓手**，两家中只有 MediaMTX 有 |
| HTTP-TS / WS-TS / HTTP-fMP4 | — | ✅ | 补 ZL 缺口，客户端少但安防常用 |
| GB/T 28181 | — | ✅ | 含双向对讲 + 主动拉流 + 级联 |
| ONVIF / PSIA | — | ✅ | WS/TS 两种 |
| JT/T 1078 | — | ✅（企业版） | |
| MPEG-DASH / CMAF (点播) | — | ✅ | 长视频点播 |
| NDI / ISUP | — | ❌ | 授权问题 |

### 6.4 性能目标（可测口径）

- 单核：≥ 1 万路 RTSP 只读广播、≥ 2 万 GOP/s 转协议扇出、≥ 5 千路 WebRTC 播放。
- 单机：≥ 5 万并发读者（HLS + CDN 前置）、≥ 5 千路 RTSP/RTMP 混合。
- 延迟：WebRTC 单跳 < 300ms（端到端），HLS-LL < 1.5s，RTSP-over-TCP < 500ms。
- 拷贝：媒体面每帧 0 次 memcpy（引用计数 + `io_uring`/Go netpoll 双通道）。

（ZL 的 `tests/test_bench_{push,pull,forward,proxy}.cpp` 是现成的 benchmark 基线，可以作为回归测试模板。）

### 6.5 里程碑建议

| 阶段 | 交付 | 参考 |
|---|---|---|
| **M0（1 周）** | PoC：`mediamtx` 二次开发骨架，跑通 RTSP→RTMP→HLS→WebRTC 全链路 | MediaMTX 已就绪 |
| **M1（1–2 月）** | v1 alpha：RTSP/RTMP/HLS/WebRTC/SRT/MPEG-TS/RTP 七协议 + Control API + Hooks + Prometheus | 直接对齐 MediaMTX v1 功能集 |
| **M2（2 月）** | v1 beta：+ MoQ、+ fMP4 录像、+ 绝对时间戳、+ always-available、+ JWT | MediaMTX v1.12+ |
| **M3（3 月）** | v1 GA：+ GB28181（含对讲）+ ONVIF + TS 透传 + 单端口复用 + 连接迁移 | 超出 MediaMTX，对齐 ZL 安防能力 |
| **M4（3–6 月）** | v2：内建转码（FFmpeg in-proc）+ 集群（溯源/边沿）+ S3 录像 + Web UI + SDK | 结合两家闭源版能力 |

---

## 7. 风险与开放问题

1. **许可证**：MediaMTX 是 MIT，可商用可修改；`bluenviron/*` 与 `pion` 亦为宽松许可。若二次开发，需要保证 `bluenviron` 上游变更的同步策略（`go.mod` 版本锁 + 定期 rebase）。
2. **GB28181 的 SIP 栈**：Go 生态没有生产级 SIP，通常做法是引入 `pion` 系或 C 的 `opensips`/`kamailio` 子进程；这是 M3 阶段最大的技术不确定项。
3. **WebRTC 浏览器兼容**：MediaMTX docs/25 已明确 **H.265 与 H.264-B-frames 在浏览器有硬伤**，QuickMedia 必须内置"浏览器友好编码"（H.264 baseline + Opus）策略或转码提示。
4. **"高性能"的宣称**：不要在没有 benchmark 数据前承诺具体数字。建议 M1 阶段先建立 `test_bench_*` 级别的回归测试（可参考 ZL 的 `tests/test_bench_forward.cpp` 13KB）。
5. **闭源/开源功能边界**：ZL 把转码/JT1078/S3 录像/AI 插件/MCU 都放在闭源版，其开源版的功能边界本身就是"开源能做什么"的市场答案。QuickMedia 若走开源路线，转码/JT1078/AI 是**必须明确放弃或社区共建**的功能。
6. **交付形态未定义**：本 issue 未指定交付形态，本报告按"技术选型 + 架构设计 + 协议矩阵 + 里程碑"给出。若后续需要商业计划书/客户价值/成本收益分析，需另立 issue。

---

## 8. 证据来源

- ZLMediaKit `README.md`（"项目特点 / 项目定位 / 功能一览 / 闭源专业版"，逐行引用）
- ZLMediaKit `conf/config.ini`（42KB 配置项枚举）
- ZLMediaKit `conf/readme.md`、`webrtc/USAGE.md`、`srt/srt.md`
- ZLMediaKit 目录树：`src/{Extension,Codec,Common,Rtsp,Rtmp,Rtp,Http,TS,FMP4,Onvif,Record}`、`webrtc/`、`srt/`、`api/{include,source}`、`server/`、`ext-codec/`、`tests/`
- MediaMTX `README.md`、`mediamtx.yml`（38KB 默认配置）
- MediaMTX `docs/2-features/{02-architecture,07-remuxing,08-always-available,11-forward,15-absolute-timestamps,19-scalability,23-performance,24-srt,25-webrtc,26-rtsp}.md`
- MediaMTX `docs/3-publish/{12-mpeg-ts,13-rtp}.md`
- MediaMTX `docs/6-misc/4-specifications.md`（含 RFC 清单：8835 / 7742 / 7874 / 7875 / 9725 / 9000 / 9114 / draft-moq-transport / MSF / LOC）
- MediaMTX 目录树：`internal/{core,protocols,servers,stream,recorder,recordstore,playback,forward,staticsources,api,auth,conf,confwatcher,hooks,metrics,ntpestimator,rlimit,restrictnetwork,test,teste2e,unit}`
- 星标数据：`img.shields.io/github/stars/*`
