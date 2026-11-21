# 真实 Redis 验证记录

验证日期：2026-11-19（Asia/Shanghai）。Windows Python 3.12 客户端连接 WSL Ubuntu-24.04 内运行的真实 Redis 7.4.12；上游模型仍是明确标记的 mock，不代表真实 LLM 接入。

## 环境准备与实际构建

环境内没有 `redis-server` / `redis-cli`，但 WSL 的 gcc、make 和 curl 可用。没有安装或修改系统包。源码从 [Redis 官方发布目录](https://download.redis.io/releases/) 下载 `redis-7.4.12.tar.gz`，归档保存在 `artifacts/redis-build/`。

下载文件 SHA-256（本地计算，便于复现比对）：

```text
56588ee1648539556f2b6cf4e873e3734afbbe40fa952fec2fcd6b6d7dae2924
```

第一次直接在 `/mnt/f/.../redis-7.4.12` 编译，遇到 Windows 挂载盘文件访问、发布脚本 git diff 扫描与 LTO 链接耗时；主动停止这次构建，日志可见 Hangup。随后在 WSL `/tmp` 的独立临时目录重新构建，使用 `GIT_DIR` 阻止仓库发现、`OPTIMIZATION=-O2` 关闭默认 LTO，成功后只把 `redis-server` / `redis-cli` 复制回项目工件目录。最终构建路径记录在 `artifacts/redis-build/linux-build-path.txt`，命令在同目录 `build.sh`，成功日志为 `build-linux.log`；旧构建日志 `build.log` 不作为成功证据。

以下是历史 Windows/WSL 本机构建命令，依赖当时的 `artifacts/redis-build/build.sh` 等临时工件。源码包、脚本、二进制和构建日志未收录到公开仓库，不能直接在新克隆目录运行此段。复现网关 Redis 验证时，请连接已有 Redis，并按 [README 的 Redis 配置](../README.md#redis-与真实供应商) 设置测试地址，再执行 `scripts/verify_redis.py --redis-url <测试地址>`。

```powershell
Set-Location F:\codexproject\llm-gateway
New-Item -ItemType Directory -Force artifacts\redis-build | Out-Null
wsl -d Ubuntu-24.04 -- curl --noproxy "*" --fail --location --max-time 120 https://download.redis.io/releases/redis-7.4.12.tar.gz -o /mnt/f/codexproject/llm-gateway/artifacts/redis-build/redis-7.4.12.tar.gz
wsl -d Ubuntu-24.04 -- sh /mnt/f/codexproject/llm-gateway/artifacts/redis-build/build.sh
wsl -d Ubuntu-24.04 -- /mnt/f/codexproject/llm-gateway/artifacts/redis-build/redis-server --version
```

实际输出：`Redis server v=7.4.12 sha=00000000:1 malloc=libc bits=64 build=4baa1a5c5d18fc1a`。发布源码包的构建标识不是本项目的 Git commit。没有运行 Redis 自身完整 `make test`；本次验证范围是网关使用的 Redis API 与 Lua 路径。

## 启动、验证与停止

已实际使用的 Redis 配置 `artifacts/redis-build/redis.conf`：仅监听 `127.0.0.1:16379`，开启 protected-mode，关闭 RDB/AOF，日志和 pid 文件放项目工件目录；这是临时验收实例。Windows 到 WSL 的 localhost 转发在本环境中已验证。

```powershell
wsl -d Ubuntu-24.04 -- /mnt/f/codexproject/llm-gateway/artifacts/redis-build/redis-server /mnt/f/codexproject/llm-gateway/artifacts/redis-build/redis.conf
$env:LLMGW_TEST_REDIS_URL = 'redis://127.0.0.1:16379/15'
.\.venv\Scripts\python.exe -m pytest tests/test_policies.py -q --junitxml=artifacts/policies-redis-junit.xml
.\.venv\Scripts\python.exe scripts/verify_redis.py --redis-url redis://127.0.0.1:16379/15
Remove-Item Env:LLMGW_TEST_REDIS_URL
# 仅停止上述专为本次验收创建的 16379 实例。
wsl -d Ubuntu-24.04 -- /mnt/f/codexproject/llm-gateway/artifacts/redis-build/redis-cli -p 16379 shutdown nosave
```

实际策略测试：**16 passed in 2.70s**，没有 skip。原始 stdout 为 `artifacts/policies-redis-output.txt`，JUnit 为 `artifacts/policies-redis-junit.xml`。

- Lua 原子性：两个独立 Redis 连接池对同一随机前缀 key 并发 64 次争抢 8 个名额，恰好放行 8 次，余下返回有效 TTL；1 秒窗口结束后可再次放行。
- 队列容量、FIFO、等待超时、取消、释放交接取消竞态、幂等 release 均通过；大量取消后 active/waiting 归零。
- 熔断最小样本、固定窗口边界、neutral 不计数、单个半开探测、取消探测、恢复、旧 token 延迟返回、重复 record、排队期间熔断打开后撤销旧 token 均通过。

HTTP 验收脚本也已实际通过，完整原始数据为 `artifacts/redis-http.json`，stdout 为 `artifacts/redis-http-output.txt`。测试创建独立网关/mock 子进程并在退出时收尾，外部 Redis 不由脚本修改或停止：

| 场景 | 实际结果 |
| --- | --- |
| 两个独立网关进程共享同一 Redis RPM key，16 个独立 HTTP 客户端，配额 8，每请求最多尝试 1 次 | 8 个 200、8 个 429；mock 上游合计只接到 8 次请求 |
| 显式 Redis 后端配置为不可连接端口 | HTTP 503 `rate_backend_unavailable`；mock 上游接到 0 次请求，未回退内存 |
| 两个成功场景与故障场景收尾 | 所有供应商 active=0、waiting=0、breaker inflight=0 |

这里启动两个网关是为了检查 **Redis RPM 的跨进程原子性**，不能据此宣称网关已支持多副本部署：并发槽与熔断器仍是进程本地状态。HTTP 故障用明确不可连接的端点，不会关闭用户已有的 Redis；不使用 `FLUSHDB`。测试键使用随机前缀并自动过期。

## 尚未覆盖

未验证 Redis 集群、故障转移、持久化恢复、ACL/TLS、跨机器网络，以及多副本分布式并发租约。这些不属于当前单实例首版交付范围。Docker/Compose 未参与此验证。
