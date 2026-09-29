# Edge API Gateway（本地可运行 / 可演练）

零外部基础设施即可在本地跑起来的边缘网关：对外提供**统一入口**，把请求按确定性规则
转发到上游。系统提供两种上游形态：

- **进程内模拟上游**（`edge_gateway/upstream.py`）：无需起服务即可快速验证路由、配额、
  故障转移逻辑；
- **本地独立网络上游**（`edge_gateway/lab/`，本轮新增）：每个上游是**可单独起停的真实
  HTTP 进程**，网关走真实 loopback 网络访问，能复现“进程被停掉 / 重启中连接拒绝 / 慢响应 /
  直接报错”等真实网络故障语义。

核心能力：

- **动态路由 / 上游选择**：按路径、方法、Header、Query、调用方匹配；显式优先级；
  冲突与无匹配都返回可区分错误。
- **三类准入（本轮细化）**：短窗口**速率**、**在途并发**、周期**总额度**三类相互独立，
  哪类不满足就按哪类拒绝，并回传三类的实时状态；被拒请求不触碰上游、也不白扣其它类别。
- **后台自愈（本轮与业务流量解耦）**：被摘除实例由后台探测线程按节奏自行恢复，**完全没有
  流量也能恢复**；需连续多次探测成功才放回；可查询每个实例最近的摘除/恢复记录（时间+原因）。
- **网络透传**：方法、路径、查询、请求体、常用请求头原样送达；上游的状态码、响应体、响应头
  原样带回。
- **故障转移与非幂等安全**：连接拒绝可安全转移；已执行 / 执行不可确认（5xx、超时）时
  非幂等请求绝不重试。
- **可解释**：每个响应和测试日志都打印输入、判定依据（trace）与最终实例选择。

## 1. 两种运行方式

### 1.1 进程内演示（仅网关一个进程）

```bash
uv sync
uv run uvicorn main:app --reload          # http://127.0.0.1:8000
```

适合快速验证路由与基础故障转移，上游是进程内的模拟对象。

### 1.2 本地网络演练（网关 + 一组独立上游进程，推荐）

一条命令把网关和**全部 6 个独立上游进程**拉起来：

```bash
uv run python -m edge_gateway.lab.orchestrator up
```

- 每个上游是独立 OS 进程，监听各自的 loopback 端口（`edge_gateway/lab/lab_config.py`）：

  | 实例 | 端口 | 实例 | 端口 |
  | --- | --- | --- | --- |
  | users-v1-1 | 9601 | orders-v1-1 | 9604 |
  | users-v1-2 | 9602 | orders-v1-2 | 9605 |
  | users-v2-1 | 9603 | orders-v2-1 | 9606 |

  网关监听 `8000`。
- 网关入口与调用方式不变（`x-caller-id` 标识调用方）：

```bash
curl -s -D - -H 'x-caller-id: bob' http://127.0.0.1:8000/users/7
# 响应头 x-gateway-instance 标明实际服务实例；响应体即上游原始响应
```

对**指定上游**做停进程 / 重启 / 慢响应 / 错误响应，随时复现问题：

```bash
uv run python -m edge_gateway.lab.orchestrator status                 # 进程与模式
uv run python -m edge_gateway.lab.orchestrator stop users-v1-1        # 杀掉一个进程
uv run python -m edge_gateway.lab.orchestrator start users-v1-1       # 单独拉起
uv run python -m edge_gateway.lab.orchestrator slow users-v1-1 1.5    # 慢响应 1.5s
uv run python -m edge_gateway.lab.orchestrator error users-v1-1       # 业务返回 500
uv run python -m edge_gateway.lab.orchestrator respond users-v1-1 418 # 返回自定义状态
uv run python -m edge_gateway.lab.orchestrator normal users-v1-1      # 恢复正常
uv run python -m edge_gateway.lab.orchestrator down                   # 全部停止
```

> 运行态的 PID 与日志写在 `.lab_state/`（已加入 `.gitignore`）。
> 网络演练入口是 `uv run uvicorn main_net:app`（编排器内部即使用它）。

## 2. 路由匹配优先级（语义未变）

规则的所有条件之间是 **AND** 语义；维度：`path_prefix`、`methods`、`header_equals`、
`query_equals`、`callers`。

1. 找出所有完全匹配的规则；
2. 取其中 `priority` **数值最大**者；
3. 最高分有**多条**并列 → `409 ROUTE_CONFLICT`，响应列出 `conflicting_rule_ids`，
   **绝不随机挑选、绝不静默丢弃**；
4. 没有任何规则匹配 → `404 NO_ROUTE`。

> 规则 id 只用于稳定展示排序，不会用来“悄悄打破”真实冲突。
> 内置规则中故意放了一对同优先级冲突规则（`conflict-alpha` / `conflict-beta`）。

集群内实例按配置固定顺序尝试，**没有随机选择**。

## 3. 准入：速率 / 在途 / 额度 三类独立

准入维度是 **(调用方 caller, 规则 rule)**，三类相互独立、各自有实时状态：

| 类别 | 含义 | 规则字段 | 拒绝错误码 |
| --- | --- | --- | --- |
| **速率 rate** | 短窗口内冲得太猛（固定窗口，默认 0.2s） | `rate_limit` / `rate_window_seconds` | `429 RATE_LIMIT_EXCEEDED` |
| **在途 inflight** | 此刻同时在途的请求太多 | `inflight_limit` | `429 INFLIGHT_LIMIT_EXCEEDED` |
| **额度 quota** | 一个周期窗口内的总额度用光（默认 1s） | `quota_limit` / `quota_window_seconds` | `429 QUOTA_EXCEEDED` |

关键保证：

- **原子判定 + 原子消费**：三类在同一临界区评估；只有**每一类都有余量**才各扣 1。
  任意一类拒绝，**三类计数都不动**——被拒请求既不占上游资源，也不白扣其它类别。
- 消费发生在调用上游**之前**，请求结束后归还“在途”名额：
  - 并发下接受数永远不超各类上限（`tests/test_admission.py` 精确断言）；
  - 上游变慢不会让额外请求挤入；上游重启（连接拒绝）同样不会超额；
  - **网络入口把同步派发放到线程池执行**，避免一个慢上游阻塞事件循环把并发请求串行化
    （回归测试 `test_event_loop_is_not_serialized_inflight_cap_holds`）。
- 拒绝响应的 `details` 同时给出 `rejected_category` 与三类的 `limit/used/remaining/窗口`：

```json
{"ok": false, "error": {"code": "INFLIGHT_LIMIT_EXCEEDED",
  "details": {"rejected_category": "inflight",
    "admission": {"rate": {"limit":4,"used":4,"remaining":0},
                  "inflight": {"limit":2,"used":2,"remaining":0},
                  "quota": {"limit":4,"used":2,"remaining":2}}}}}
```

> 进程内演示（`main:app`）只强制“周期额度”一类，以保持旧行为；
> 网络演练（`main_net:app`）的规则三类全部配置，见 `lab_config.default_rules()`。

## 4. 健康摘除、后台自愈与故障转移

每个实例是 `CLOSED / OPEN / PROBING` 状态机：

```
CLOSED（可服务）
   │ 连续失败达到 failure_threshold（默认 3）
   ▼
OPEN（已摘除；一次性产生“摘除”事件，重复失败不重复摘除）
   │ 距摘除达到 probe_interval，探测被原子认领（每实例最多一个在途探测）
   ▼
PROBING → 探测：
   ├─ 失败：恢复连胜清零，回 OPEN 重新计时
   └─ 成功：连胜 +1；未达 recovery_threshold（默认 2）仍留 OPEN，
            连续达到才回 CLOSED 并产生“恢复”事件
```

**后台自愈（与业务流量解耦）**：`BackgroundProber` 守护线程按固定间隔主动认领并执行
到期探测。因此一段时间内**完全没有请求**，被摘掉的实例也会按节奏自行探测回来，不需要等
下一次访问。它与“随流量的机会式探测”共用同一套原子认领，所以：

- 并发探测下每个实例最多一个在途探测，**不会重复摘除**；
- 必须**连续多次**探测成功才放回，一次侥幸不会放回，**不会 OPEN↔CLOSED 抖动**；
- 任一探测失败立即清零连胜。

**摘除/恢复记录**：`GET /__admin/state` 返回每个实例的最近摘除/恢复时间与原因，以及
追加式事件流（`events`，含 `instance_id / kind(removed|recovered) / reason / timestamp`）。

### 网络故障语义与非幂等安全

| 上游情形 | 网络表现 | 幂等方法 (GET/HEAD/OPTIONS) | 非幂等方法 (POST/…) |
| --- | --- | --- | --- |
| 进程停止 / 重启中 | TCP 连接拒绝（尚未执行） | 尝试下一实例 | 可安全尝试下一实例 |
| 业务返回 5xx | 已收到响应（**已执行**） | 尝试下一实例 | `502 NON_IDEMPOTENT_NOT_RETRIED`，绝不重试 |
| 慢响应超过 timeout | 超时（是否执行**不可确认**） | 尝试下一实例 | `502 NON_IDEMPOTENT_NOT_RETRIED` |
| 其它传输错误 | 连接重置等 | 尝试下一实例 | `502`，不重试 |

- 尝试次数上限 `max_attempts=3`；全部不可用 → `503 ALL_UPSTREAMS_UNAVAILABLE`。
- 非幂等“最多执行一次”由上游的**业务请求执行计数**（`GET /__meta`）在测试中断言。
- 探测走 `GET /__health`，不命中业务处理，不会污染执行计数。

## 5. 网络透传细节

- **请求**：原样转发 HTTP 方法、路径、原始查询串、请求体与常用请求头；剔除
  hop-by-hop 头（`connection / host / content-length / transfer-encoding` 等）。
- **响应**：原样返回上游状态码、响应体与内容相关响应头，并附加
  `x-gateway-rule`、`x-gateway-instance` 便于观测。
- 上游业务处理会回显收到的方法/路径/头/体，可直接据此断言透传是否完整。

## 6. 可观测 / 管理端点

| 方法与路径 | 说明 |
| --- | --- |
| `GET /healthz` | 网关存活（网络演练还回报后台探测器是否运行） |
| `GET /__routes` | 规则与集群 |
| `GET /__explain?path=...&method=...` | 仅进程内演示：路由解释，不转发 |
| `GET /__admin/state` | 健康状态 + 摘除/恢复事件 + 探测器状态 |
| `POST /__admin/reset` | 重置准入计数与健康状态 |
| `POST /__admin/faults` | 仅进程内演示：注入 fail/timeout/down 故障 |

错误响应统一为：

```json
{"ok": false, "error": {"code": "ROUTE_CONFLICT", "message": "...", "details": {...}}}
```

错误码：`NO_ROUTE(404)`、`ROUTE_CONFLICT(409)`、`RATE_LIMIT_EXCEEDED(429)`、
`INFLIGHT_LIMIT_EXCEEDED(429)`、`QUOTA_EXCEEDED(429)`、
`NON_IDEMPOTENT_NOT_RETRIED(502)`、`ALL_UPSTREAMS_UNAVAILABLE(503)`。

## 7. 本地验证

```bash
uv run pytest -s                                   # 全量测试
uv run pytest tests/test_router.py -s              # 路由优先级/冲突/无匹配（不回归）
uv run pytest tests/test_failover.py -s            # 进程内故障转移/非幂等/探测
uv run pytest tests/test_admission.py -s           # 三类准入边界 + 并发不超额
uv run pytest tests/test_self_heal.py -s           # 无流量自愈/事件/单次摘除/防抖
uv run pytest tests/test_network_upstream.py -s    # 真实 HTTP 透传/停进程/重启自愈
uv run pytest tests/test_orchestrator.py -s        # 真实独立子进程编排（起停/慢/错）
```

测试共 54 个，其中：

- `tests/test_network_upstream.py` 用**真实 loopback HTTP 服务**（同一 `upstream_server`
  模块，测试中以线程承载，CI 稳定）断言：透传、停进程连接拒绝与安全故障转移、
  重启后零流量自愈、非幂等 5xx/超时只执行一次、事件循环不串行化；
- `tests/test_orchestrator.py` 用**真实独立 OS 子进程**断言：每个上游可单独起停，
  停一个后流量转移、重启后自动回到原实例、慢/错模式可控。

手工演练脚本见 §1.2。一个典型的“停进程 → 观察摘除 → 重启 → 零流量自愈”：

```bash
uv run python -m edge_gateway.lab.orchestrator up
uv run python -m edge_gateway.lab.orchestrator stop users-v1-1
for i in 1 2 3 4; do curl -s -D - -o /dev/null -H 'x-caller-id: b' \
  http://127.0.0.1:8000/users/1 | grep -i x-gateway-instance; done   # 都落到 v1-2
curl -s http://127.0.0.1:8000/__admin/state | python3 -m json.tool   # 看到 removed 事件
uv run python -m edge_gateway.lab.orchestrator start users-v1-1      # 重启，期间不发请求
sleep 1
curl -s http://127.0.0.1:8000/__admin/state | python3 -m json.tool   # 出现 recovered 事件
```

## 8. 兼容范围

- **路由语义、优先级、冲突与无匹配结论保持不变**，由 `tests/test_router.py` 持续守护。
- **进程内演示形态（`main:app`）保持可用**：只强制周期额度，旧的 fail/timeout/down
  故障注入与 `/__explain`、`/__admin/faults` 等端点不变；`tests/test_http.py`、
  `tests/test_quota.py`、`tests/test_failover.py` 全部保持通过。
- 新增的网络演练形态（`main_net:app` / `edge_gateway.lab`）是**叠加**能力：三类准入、
  后台自愈、透传响应头不影响旧形态对外契约。
- 唯一新增的内部接口变化：准入检查统一为 `(allowed, snapshot, trace, category)` 四元组，
  并新增 `release()`（进程内配额管理器的 `release` 为空操作）；`ForwardResult` 增加
  原始响应体/响应头字段，旧字段保留。

## 9. 代码结构

```
edge_gateway/
  models.py          # Rule（含 rate/inflight 字段）/ RequestContext / HealthEvent / ForwardResult
  errors.py          # 可区分错误码（含 RATE/INFLIGHT 两类新增）
  config.py          # 进程内演示规则与集群
  upstream.py        # 进程内模拟上游 + 故障注入 + 执行计数 + 统一 send() 传输接口
  router.py          # 规则匹配、优先级、冲突/无匹配与 explain
  quota.py           # 进程内演示：周期固定窗口配额（四元组协议，release 空操作）
  admission.py       # 网络演练：rate/inflight/quota 三类原子准入（线程安全）
  health.py          # 状态机 + 原子探测认领 + 摘除/恢复事件（时间+原因）
  prober.py          # 后台自愈探测线程（与业务流量解耦）
  http_upstream.py   # 真实 HTTP 传输：连接拒绝/超时映射、方法路径查询头体透传
  gateway.py         # 路由→准入→健康感知分发→故障转移 的主流水线（透传）
  app.py             # 进程内演示 FastAPI 入口与管理端点
  lab/
    upstream_server.py  # 独立上游进程：回显业务 + /__health + /__control/* 控制
    lab_config.py       # 演练端口/集群/三类准入规则
    netapp.py           # 透传网关 ASGI（线程池派发）+ 后台探测器生命周期
    orchestrator.py     # 一键起停整套进程 / 单实例起停 / 慢错模式控制
main.py              # 进程内演示入口：uvicorn main:app
main_net.py          # 网络演练入口：uvicorn main_net:app
tests/               # 路由 / HTTP / 配额 / 故障转移 / 准入 / 自愈 / 网络 / 编排
```
