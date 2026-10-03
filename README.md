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
- 网络模式的并发与隔离语义见 **第 5 节**（重要：网关主管线跑在独立工作
  线程池，慢上游不会串行化请求或冻结入口）。

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

## 5. 真实网络模式的并发语义

### 5.1 并发请求真正并行（不再排队）

网关主管线（路由 → 准入 → 阻塞式 TCP 上游调用 → 故障转移）是同步代码，
网络模式下底层是阻塞 socket（`http.client`）。它**不会**在 asyncio 事件
循环线程上执行：每个入站请求都被提交到网关进程内的**专用有界工作线程池**
（`EG_DISPATCH_WORKERS`，默认 32 个线程），由工作线程完成阻塞等待。

因此：

- 同一时刻进来的多个请求被**真正并行**处理：两个各自 ~0.5s 的慢请求同时
  发出时，总耗时接近 **0.5s（单个慢请求）**，而不是 ~1.0s（逐个累加）。
- 上游 socket 等待发生在工作线程里，**事件循环线程永远不被阻塞调用占住**。
- 连网络模式下经网络下发的 `POST /__admin/faults`（`set_remote_mode` 本身
  也是一次阻塞 HTTP 调用）也在同一工作池执行，不占用事件循环。

> 修复前的行为：`gateway.handle(...)` 直接在 async handler 里同步执行，
> 一个慢上游的 `getresponse()` 会占死整个事件循环，表现为并发请求排队、
> 健康/状态接口一起冻结、inflight 准入在真实网络入口永远打不满。

### 5.2 某个上游变慢 / 卡住 / 重启时的预期

| 场景 | 预期 |
| --- | --- |
| 一个上游慢响应（在请求超时内） | 打到它的请求并行等待；**其它请求、其它路径照常即时处理** |
| 一个上游卡住不返回 | 占用至多 1 个工作线程直到 `EG_REQUEST_TIMEOUT`（默认 2s）→ 按超时故障转移/拒绝；期间事件循环与其余请求不受影响 |
| 一个上游正在重启（连接拒绝） | 立即得到 `UpstreamUnreachable`，幂等请求当场转移到下一实例；不等待 |
| 全部上游都慢/不可用 | 只有走到上游的请求受影响（各自占工作线程并受超时上界约束）；`GET /healthz`、`GET /__admin/state`、`/__explain`、`/__routes` 与**无匹配路由**（不走上游）始终及时响应 |
| 慢/重启持续较久 | 健康状态机按既有阈值摘除与后台探测自愈；准入窗口与判定不受影响 |

边界与上界：工作线程数有界（默认 32），上游调用有 socket 超时上界
（`EG_REQUEST_TIMEOUT`），所以即使所有上游同时冻结，被占住的资源也是
有限且会自动归还的，不会无限堆积。

### 5.3 三类准入在并发下的真实网络预期

三类判定仍然在**同一把锁、同一个临界区**内"先逐项判定、全部通过再一起
消费"，而消费发生在进入上游之前；真实网络入口的并发让"同时在途"成为
常态，三类限制都能按各自边界真实触发：

以 `payments-api`（rate=3/1s、inflight=2、quota=5/3s）为例，同一调用方：

| 类别 | 真实网络入口下的触发方式 | 边界 | 被拒之后 |
| --- | --- | --- | --- |
| **rate 短突发** | 同一个 1s 窗口内超过 3 个请求**起步** | 窗口内恰好 3 个进入上游，第 4+ 个 `429 RATE_LIMITED` | 不执行上游、不消耗任何类别 |
| **inflight 在途** | 2 个请求正在慢上游里执行时，再来请求 | 上游内同时最多 2 个；其余 `429 INFLIGHT_LIMIT_EXCEEDED` | 不执行上游；在途请求结束后槽位立即归还 |
| **quota 周期额度** | 3s 窗口内累计起步满 5 个后，在新 burst 窗口再发请求（rate 已放行） | 第 6 个 `429 QUOTA_EXCEEDED`（响应里 rate 仍有剩余） | 不执行上游；额度不退还，随窗口滚动恢复 |

- 并发、上游变慢或重启期间**绝不超额**：准入在调用上游之前的临界区内完成，
  被拒请求根本不会建立到上游的连接。
- 多类别同时触顶时按固定顺序 `rate → inflight → quota` 给出唯一拒绝类别，
  响应体同时携带三类的 `limit/used(inflight)/remaining/window_*` 快照。
- 用上游真实进程的用户请求执行计数（`/__admin/state` 的 `calls`）可直接
  核对"被拒请求零上游执行、放行数恰好等于边界"。

### 5.4 本地如何复现与验证

一键起环境（网关 19080，见 1.2）：

```bash
uv run python -m edge_gateway.drill up
uv run python -m edge_gateway.drill slow users-v1-1 0.5   # 每个响应真实慢 0.5s
```

```bash
# (1) 并发并行：两个慢请求同时发，总耗时应接近 0.5s 而非 1.0s
time (curl -s --noproxy '*' -H 'x-caller-id: c1' http://127.0.0.1:19080/users/1 &
      curl -s --noproxy '*' -H 'x-caller-id: c2' http://127.0.0.1:19080/users/2 &
      wait)

# (2) 慢请求在途时，健康/状态接口立即返回（另开终端同时执行）
curl -s --noproxy '*' http://127.0.0.1:19080/healthz
curl -s --noproxy '*' http://127.0.0.1:19080/__admin/state
curl -s -o /dev/null -w '%{http_code}\n' --noproxy '*' http://127.0.0.1:19080/unrelated

# (3) inflight=2：5 个 /payments 同时打到慢上游，恰好 2 个 200、其余 429
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w '%{http_code}\n' --noproxy '*' -H 'x-caller-id: pay' \
    http://127.0.0.1:19080/payments/1 &
done; wait
uv run python -m edge_gateway.drill status   # 看 users-v1-1 的 calls 增量

uv run python -m edge_gateway.drill normal users-v1-1
uv run python -m edge_gateway.drill down
```

自动化验证（真实上游进程 + 真实 asyncio 网络入口，无需手起服务）：

```bash
uv run pytest tests/test_network_concurrency.py -sv   # 见下
```

该文件覆盖并在日志中逐条打印用例输入/边界/结果：

- `test_two_concurrent_slow_requests_run_in_parallel` —
  两个 0.5s 慢请求并行总耗时 < 0.85s（串行会是 1.0s），且都真实到达上游；
- `test_slow_upstream_does_not_freeze_gateway_endpoints` —
  慢请求与 `/healthz`、`/__admin/state` 同时刻发出，后者在毫秒级完成
  （修复前会被推迟到慢请求结束，即 ~0.5s）；
- `test_slow_upstream_does_not_block_unrelated_requests` —
  慢请求在途时，不相干地址（NO_ROUTE，不走上游）仍立即返回；
- `test_rate_boundary_rejects_burst_beyond_limit_without_overadmit` —
  同一 burst 窗口恰好 3 个进入上游，其余 RATE 拒绝，上游执行次数 == 3；
- `test_inflight_boundary_holds_under_concurrent_slow_upstream` —
  5 个并发打慢上游，恰好 2 个 200、3 个 429（含 INFLIGHT 拒绝），上游执行
  次数 == 2，总耗时 ≈ 单个慢请求；
- `test_quota_boundary_rejects_after_window_budget_exhausted` —
  耗尽 5 个周期额度后，新 burst 窗口内 rate 仍有余量但被 QUOTA 拒绝，
  上游执行次数恰为 5。

## 6. 两种形态的响应差异

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

## 7. 管理 / 可观测端点

| 方法与路径 | 说明 |
| --- | --- |
| `GET /healthz` | 网关存活（返回当前 `mode`） |
| `GET /__routes` | 全部规则（含三类限额）与集群 |
| `GET /__explain?path=...&method=...` | 只做路由解释，不转发 |
| `GET /__admin/state` | 每实例健康状态 + 摘除/恢复事件（时间、原因） |
| `POST /__admin/faults` | 模拟模式：`fail/timeout/down/null`；网络模式：`error/slow/null`（真停进程用 drill CLI） |
| `POST /__admin/reset` | 重置准入与健康（模拟模式还清故障与计数） |

## 8. 兼容范围

- 模拟模式的全部既有行为（路由优先级、冲突识别、无匹配、固定窗口配额、故障转移结论）保持不变：
  `quota_limit/quota_window_seconds` 字段语义不变；三类准入在未配置 rate/inflight 时退化为原配额语义。
- 网络模式复用**同一路由/准入/健康/故障转移内核**，仅替换上游传输层与响应封装，
  因此两形态的判定结论一致。
- 上游进程协议：回显 HTTP/1.1，支持 GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS；
  探测端点 `GET /__health`（正常/慢模式返回 200，错误模式返回 503）。
- 仅依赖 Python 标准库 + FastAPI/httpx（测试用）；上游进程与网关均在本机回环地址运行。

## 9. 本地验证

```bash
uv run pytest -q                                  # 全量（含真实进程与一键演练，约 40–50s）
uv run pytest tests/test_router.py -q             # 路由优先级/冲突/无匹配（不回归）
uv run pytest tests/test_admission.py -q          # 三类准入边界 + 并发不超额
uv run pytest tests/test_recovery.py -q           # 无流量后台自愈 / 事件记录 / 探测去重
uv run pytest tests/test_network_upstream.py -q   # 真实进程停/重启恢复、慢/错、透传
uv run pytest tests/test_network_http.py -q       # 网络模式 HTTP 透传 + 后台自愈集成
uv run pytest tests/test_network_concurrency.py -sv  # 真实网络入口并发并行/慢上游隔离/三类准入边界
uv run pytest tests/test_drill.py -q              # 一键起停 / 停进程 / 慢响应 / 错误
```

手工演练（先 `uv run python -m edge_gateway.drill up`）：

```bash
# 原样透传（回显 method/path/query/header/body）
curl -s -H 'x-caller-id: alice' 'http://127.0.0.1:19080/users/1?debug=1'

# 三类准入：顺序连发 4 个 /payments（每个快速返回、槽位已释放），
# 同一 1s burst 窗口内第 4 个应为 429 RATE_LIMITED
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

## 10. 代码结构

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
  app.py           # FastAPI：app（模拟）与 network_app（真实上游，含后台自愈）；
                   #         同步主管线在专用工作线程池执行，事件循环永不阻塞
tests/             # 路由 / 准入 / 故障转移 / 自愈 / 真实进程 / HTTP / 一键演练
                   #   test_network_concurrency.py: 真实网络入口的并发与三类准入边界
```
