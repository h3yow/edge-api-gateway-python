# Edge API Gateway（本地可运行 / 可演练）

一个零外部基础设施依赖的本地边缘网关：对外提供**统一入口**，把请求按确定性规则转发到上游，
并在并发访问下给出一致、可解释的路由、准入与故障转移结果。

从 v0.2 起支持**两种上游形态**：

| 形态 | 上游 | 适用 | 成功响应 |
| --- | --- | --- | --- |
| 模拟模式（默认，`app`） | 进程内模拟实例 | 快速单测/逻辑演练，无需起进程 | 可解释 JSON 信封 |
| **网络模式**（`network_app`） | **可独立起停的本地 HTTP 进程** | 真实网络故障演练（连接拒绝、慢响应、真实 5xx） | **状态码/响应体/响应头原样透传** |

核心能力：

- **确定性路由**：按路径、方法、Header、Query、调用方匹配；显式优先级；冲突与无匹配返回可区分错误。
- **三类细粒度准入（按调用方）**：短时突发速率（rate）、同时在途数（inflight）、周期总额度（quota）；
  哪一类不满足就按哪一类拒绝，并给出三类各自的当前状态；拒绝不占用上游、也不白扣其它类别额度。
- **后台自愈**：摘除的实例由守护线程按节奏独立探测恢复，**完全无业务流量也能自己回来**；
  必须连续多次探测成功才放回；并发探测去重、不重复摘除、不来回抖；保留带时间和原因的摘除/恢复记录。
- **真实故障语义**：上游进程被停（连接拒绝）、重启中、慢响应（socket 超时）、直接报 5xx 时，
  该转移的转移、该拒绝的拒绝；**非幂等请求绝不被重复执行**。
- **一键本地演练**：一条命令拉起网关 + 6 个独立上游进程，并能对指定上游做停进程/慢响应/错误响应。

## 1. 两种运行方式

### 1.1 模拟模式（零进程，快速）

```bash
uv sync
uv run uvicorn main:app --reload
# http://127.0.0.1:8000
```

### 1.2 网络模式（真实独立上游 + 一键演练）

```bash
# 一键拉起：6 个独立上游进程 + 1 个网关进程（网关在 127.0.0.1:19080）
uv run python -m edge_gateway.drill up

# 对指定上游注入真实故障（操作的是真实进程，经网络下发）
uv run python -m edge_gateway.drill stop users-v1-1     # 真正 kill 掉进程（连接拒绝）
uv run python -m edge_gateway.drill start users-v1-1    # 单独重启
uv run python -m edge_gateway.drill slow users-v1-1 3   # 每个响应慢 3 秒
uv run python -m edge_gateway.drill error users-v1-1    # 每个响应返回真实 500
uv run python -m edge_gateway.drill normal users-v1-1   # 恢复正常
uv run python -m edge_gateway.drill status              # 进程/端口/健康/故障模式/执行计数
uv run python -m edge_gateway.drill down                # 全部停掉并清理状态文件
```

- 每个上游是独立 OS 进程：`python -m edge_gateway.realupstream --id <id> --port <port>`，
  可单独启动/单独杀掉，网关通过真实 TCP（`http.client`）访问。
- 固定端口拓扑见 `edge_gateway/config.py`（上游 19101–19106，网关 19080）。
- 上游是**回显服务**：原样返回它收到的 method/path/query/常用 header/body；
  并提供 `/__health`、`/__admin/state`、`/__admin/mode` 供探测与故障注入。
- 也可只起网关（不起上游，此时所有上游不可用）：
  `uv run uvicorn edge_gateway.app:network_app --port 19080`。

调用方通过 `x-caller-id` Header 传递（缺省为 `anonymous`）。

## 2. 路由匹配优先级（未变化）

规则的所有条件之间是 **AND** 语义；维度：`path_prefix`、`methods`、
`header_equals`、`query_equals`、`callers`。

1. 找出所有完全匹配的规则；2. 取 `priority` **数值最大**者；
3. 最高分**多条并列** → `409 ROUTE_CONFLICT`（列出 `conflicting_rule_ids`，绝不随机挑选）；
4. 没有任何规则匹配 → `404 NO_ROUTE`。

内置规则见 `edge_gateway/config.py`。除原规则外，新增一条同时承载三类准入的演示规则：

| 规则 | 优先级 | 匹配 | 集群 | rate / inflight / quota |
| --- | --- | --- | --- | --- |
| users-canary | 500 | GET `/users` 且 `x-canary: true` | users-v2 | – / – / 10·1s |
| users-write | 400 | 写方法 `/users` | users-v1 | – / – / 5·1s |
| **payments-api** | 350 | `/payments` | users-v1 | **3·1s 突发 / 2 在途 / 5·3s 总额度** |
| users-read | 300 | GET `/users` | users-v1 | – / – / 8·1s |
| orders-vip | 250 | `/orders` 且 caller=vip | orders-v2 | – / – / 6·1s |
| orders-default | 200 | `/orders` | orders-v1 | – / – / 4·1s |
| conflict-alpha/beta | 100 | `/conflict` 且 `x-experiment: A`（并列） | — | 无 |

> 未设置某类限额即该类不限。集群内实例按配置固定顺序尝试，没有随机选择。

## 3. 三类准入语义（按调用方 + 规则）

在**同一把锁、同一个临界区**内完成"先逐项判定、全部通过再一起消费"：

| 类别 | 含义 | 拒绝错误码 | 何时释放 |
| --- | --- | --- | --- |
| **rate** | 短突发窗口内起步请求太多（`rate_limit` / `rate_window_seconds`） | `RATE_LIMITED` (429) | 窗口滚动 |
| **inflight** | 同一调用方该规则**同时在途**请求太多（`inflight_limit`） | `INFLIGHT_LIMIT_EXCEEDED` (429) | 响应返回（含上游报错）后立即归还 |
| **quota** | 一个周期内总额度用光（`quota_limit` / `quota_window_seconds`） | `QUOTA_EXCEEDED` (429) | 窗口滚动（不退还） |

性质：

- 三类**相互独立**：被某一类拒绝时，响应同时给出 `rate / inflight / quota` 三类的
  `limit / used(or inflight) / remaining / window_*`，并以 `rejected_category` 指明是哪一类。
- **拒绝不消费任何类别**，也绝不触碰上游（被拒请求不占上游资源）。
- **并发、上游变慢、上游重启期间都不超额**：判定+消费在调用上游之前的临界区内完成；
  慢上游占着 inflight 槽位时，多余请求在进入上游前就被拒绝。
- 固定窗口滚动后预算恢复；规则未配置某类限额时该类直通。

## 4. 健康摘除、后台自愈与故障转移

每个实例是一个状态机：

```
CLOSED（可服务）
   │ 连续失败达到 failure_threshold
   ▼
OPEN（已摘除；只产生一次 removed 事件，重复失败不重复摘除）
   │ 距摘除达到 probe_interval，探测被原子认领（全局最多一个在途探测/实例）
   ▼
PROBING → 探测：
   ├─ 失败：恢复连胜清零 → OPEN 重新计时（记录 probe_failed 事件）
   └─ 成功：连胜 +1；未达 recovery_threshold 仍留 OPEN；
            连续 recovery_threshold 次成功才回到 CLOSED（记录 recovered 事件）
```

**后台自愈（与业务流量解耦）**

- 网络模式下网关启动一个守护线程（`RecoveryLoop`，默认每 0.25s 一轮），
  主动领取到期探测并执行；**即使长时间没有任何请求，OPEN 实例也会按节奏自己被探测回来**。
- 后台探测与请求触发的机会性探测共用同一套原子认领，二者并发也不会重复探测、不会状态来回抖。
- 必须连续 `recovery_threshold`（网络模式默认 2）次成功才放回，一次侥幸不算数。
- `GET /__admin/state` 可查每个实例的状态与最近事件，事件含
  `kind`（removed / recovered / probe_failed）、`reason`、`time`（墙上时间）、`monotonic_at`。

**故障转移与非幂等安全（真实网络语义）**

| 上游真实情况 | 网络表现 | 幂等方法 (GET/HEAD/OPTIONS) | 非幂等方法 (POST/…) |
| --- | --- | --- | --- |
| 进程被停 / 重启中 | 连接拒绝/重置 → `UpstreamUnreachable`（handler 未执行） | 尝试下一实例 | 可安全尝试下一实例 |
| 进程很慢 | socket 超时 → `UpstreamTimeout`（是否执行不可确认） | 尝试下一实例 | 直接 `502 NON_IDEMPOTENT_NOT_RETRIED`，绝不重试 |
| 进程返回 5xx | 真实 HTTP 5xx（已执行一次） | 尝试下一实例 | 直接 `502`，绝不重试 |

- 尝试上限 `max_attempts=3`；集群全不可用 → `503 ALL_UPSTREAMS_UNAVAILABLE`（带尝试明细）。
- 非幂等"最多执行一次"由上游真实进程的用户请求执行计数（`/__admin/state` 的 `calls`）在测试中断言。

## 5. 两种形态的响应差异

- 模拟模式成功响应是可解释信封：`rule_id / cluster / instance_id / upstream_status /
  data / quota / admission / attempts / trace`。
- **网络模式成功响应原样透传**：上游的状态码、响应体、`content-type` 等响应头直接返回；
  网关仅追加 `x-gateway-rule`、`x-gateway-instance` 两个响应头便于核对。
- 错误响应在两种形态下结构一致：

```json
{"ok": false, "error": {"code": "RATE_LIMITED", "message": "...", "details": {...}}}
```

错误码：`NO_ROUTE(404)`、`ROUTE_CONFLICT(409)`、`RATE_LIMITED(429)`、
`INFLIGHT_LIMIT_EXCEEDED(429)`、`QUOTA_EXCEEDED(429)`、
`NON_IDEMPOTENT_NOT_RETRIED(502)`、`ALL_UPSTREAMS_UNAVAILABLE(503)`。

## 6. 管理 / 可观测端点

| 方法与路径 | 说明 |
| --- | --- |
| `GET /healthz` | 网关存活（返回当前 `mode`） |
| `GET /__routes` | 全部规则（含三类限额）与集群 |
| `GET /__explain?path=...&method=...` | 只做路由解释，不转发 |
| `GET /__admin/state` | 每实例健康状态 + 摘除/恢复事件（时间、原因） |
| `POST /__admin/faults` | 模拟模式：`fail/timeout/down/null`；网络模式：`error/slow/null`（真停进程用 drill CLI） |
| `POST /__admin/reset` | 重置准入与健康（模拟模式还清故障与计数） |

## 7. 兼容范围

- 模拟模式的全部既有行为（路由优先级、冲突识别、无匹配、固定窗口配额、故障转移结论）保持不变：
  `quota_limit/quota_window_seconds` 字段语义不变；三类准入在未配置 rate/inflight 时退化为原配额语义。
- 网络模式复用**同一路由/准入/健康/故障转移内核**，仅替换上游传输层与响应封装，
  因此两形态的判定结论一致。
- 上游进程协议：回显 HTTP/1.1，支持 GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS；
  探测端点 `GET /__health`（正常/慢模式返回 200，错误模式返回 503）。
- 仅依赖 Python 标准库 + FastAPI/httpx（测试用）；上游进程与网关均在本机回环地址运行。

## 8. 本地验证

```bash
uv run pytest -q                                  # 全量（含真实进程与一键演练，约 40–50s）
uv run pytest tests/test_router.py -q             # 路由优先级/冲突/无匹配（不回归）
uv run pytest tests/test_admission.py -q          # 三类准入边界 + 并发不超额
uv run pytest tests/test_recovery.py -q           # 无流量后台自愈 / 事件记录 / 探测去重
uv run pytest tests/test_network_upstream.py -q   # 真实进程停/重启恢复、慢/错、透传
uv run pytest tests/test_network_http.py -q       # 网络模式 HTTP 透传 + 后台自愈集成
uv run pytest tests/test_drill.py -q              # 一键起停 / 停进程 / 慢响应 / 错误
```

手工演练（先 `uv run python -m edge_gateway.drill up`）：

```bash
# 原样透传（回显 method/path/query/header/body）
curl -s -H 'x-caller-id: alice' 'http://127.0.0.1:19080/users/1?debug=1'

# 三类准入：连发 4 个 /payments，第 4 个应为 429 RATE_LIMITED
for i in 1 2 3 4; do
  curl -s -o /dev/null -w '%{http_code}\n' -H 'x-caller-id: pay' \
    http://127.0.0.1:19080/payments/1
done

# 停掉真实进程 -> 故障转移到 users-v1-2；重启后不发请求，后台自愈放回
uv run python -m edge_gateway.drill stop users-v1-1
curl -s -D- -o /dev/null -H 'x-caller-id: bob' http://127.0.0.1:19080/users/1
uv run python -m edge_gateway.drill start users-v1-1
sleep 2
curl -s -H 'x-caller-id: bob' http://127.0.0.1:19080/__admin/state   # 看 events

uv run python -m edge_gateway.drill down
```

> 若本机/环境设置了 HTTP 代理，对 `127.0.0.1` 的 `curl` 请加 `--noproxy '*'`，
> 以免演练流量被代理截走。

## 9. 代码结构

```
edge_gateway/
  models.py        # RouteRule（三类限额）/ AdmissionPolicy / RequestContext / ForwardResult
  errors.py        # 七类可区分网关错误（含三类准入拒绝）
  config.py        # 内置规则、集群、网络固定端口拓扑
  upstream.py      # 进程内模拟上游 + 故障注入 + 执行计数（模拟模式）
  realupstream.py  # 独立 HTTP 上游进程 + 网络传输（透传/连接拒绝/超时）+ 远程故障管理
  router.py        # 规则匹配、优先级、冲突/无匹配判定与 explain
  admission.py     # rate/inflight/quota 三类原子准入（线程安全）
  health.py        # CLOSED/OPEN/PROBING 状态机、探测去重、防抖、摘除/恢复事件
  recovery.py      # 与流量解耦的后台自愈守护线程
  gateway.py       # 路由→准入→健康感知分发→故障转移 主流水线（两种传输）
  drill.py         # 一键拉起/停止网关+上游进程、停/慢/错指定上游的演练 CLI
  app.py           # FastAPI：app（模拟）与 network_app（真实上游，含后台自愈）
tests/             # 路由 / 准入 / 故障转移 / 自愈 / 真实进程 / HTTP / 一键演练
```
