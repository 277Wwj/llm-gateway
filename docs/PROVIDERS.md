# 供应商协议与能力边界

核对时间：2026-11-19。首版实现一个 HTTP Chat Completions 协议适配器，提供 OpenAI、DeepSeek 两家真实服务的配置路径。这是共用兼容协议的两条接入路径，并非两个独立 SDK，也不是已经完成两家真实调用。当前没有提供真实凭据；全部运行验收使用明确标记的 mock。没有扫描其他项目寻找密钥。

| 配置项 | OpenAI | DeepSeek |
| --- | --- | --- |
| base_url | `https://api.openai.com/v1` | `https://api.deepseek.com` |
| model 示例 | `gpt-4.1-mini` | `deepseek-flash` |
| 服务端凭据环境变量 | `OPENAI_API_KEY` | `DEEPSEEK_API_KEY` |
| 特定适配 | Chat Completions 标准请求 | 服务端固定注入 `thinking: {type: disabled}` |
| 网关开放能力 | 文本、非流式函数调用、JSON 对象、文本 SSE | 同左 |

OpenAI 模型页列出 GPT-4.1 mini 支持 Chat Completions、streaming、function calling 和 structured outputs，但当前标记 deprecated；该示例并不承诺账号可用性。取得凭据后必须检查模型可用性，并按模型能力更新配置。不要直接替换为推理模型并假设 `temperature`、`max_tokens` 等参数保持兼容。[OpenAI 模型页](https://developers.openai.com/api/docs/models/gpt-4.1-mini)、[Chat Completions 官方接口](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)。

DeepSeek 当前官方示例使用 `deepseek-flash` / `deepseek-v4-pro`。未显式设置时会启用 thinking；本项目从供应商配置关闭 thinking，保留非思考模式。JSON 对象需要消息提示输出 JSON；工具参数由应用在执行前验证，网关不会执行工具。[DeepSeek 接入说明](https://api-docs.deepseek.com/en/)、[Chat Completions 官方接口](https://api-docs.deepseek.com/api/create-chat-completion/)。

只接受网关已声明的请求字段和模型能力；并不宣称完全兼容供应商功能。流式工具调用、多模态、推理增量、Responses API、供应商 web tools、JSON Schema strict 模式不在首版支持范围。`response_format={type:json_object}` 只保证返回 JSON 对象，非业务 schema 或事实准确性；AgentRAG 自己继续验证引用和结果 schema。

## 连接与失败语义

- 每次上游请求只使用配置的服务端 key；客户端 Authorization 仅用于网关准入。请求 ID 转发到上游；key、prompt 和上游响应错误正文不进入错误消息。
- 连接池由服务生命周期持有。普通响应解码后最多 2 MiB；单个 SSE 事件最多 256 KiB，包含注释和未知字段。
- SSE 按字节增量解析，支持 UTF-8 被拆分、CRLF 被拆分、多个 `data:` 行、注释。预读并验证第一个 JSON chunk 才交付下游响应。role-only chunk 是合法首事件；首个内容时间单独记录。
- 成功要求 finish chunk 与 `[DONE]`。EOF、半个事件、上游中断标志、工具增量、非法 JSON 都作为失败。已交付的流不会换模型续接。调用方在 finally 关闭响应；适配器也在 EOF、异常、超时、取消和正常 DONE 上关闭。
- 408、429、500、502、503、504 以及连接错误/超时是有限重试候选，仍受主服务统一截止时间和尝试次数限制。400/413/422、401/403 和其他状态不自动重试；客户端参数和上游鉴权错误不计熔断故障。所有重定向拒绝跟随。
- `Retry-After` 支持秒数和 HTTP 日期，只返回提示值；统一调度必须检查剩余预算，不能延长逻辑请求截止时间。取消连接无法保证供应商停止推理或不计费。

## 真实接入验收下一步

为网关设置不同于供应商密钥的私有 `GATEWAY_API_KEY`，设置 `GATEWAY_MODE=real`、`GATEWAY_CONFIG=configs/real.json`，分别设置两个上游环境变量，然后使用 README 命令启动。缺配置启动失败；供应商失败返回脱敏错误，不切换 mock。先用 `openai-chat` 和 `deepseek-chat` 单路别名逐一验证文本、JSON、工具、SSE，再用 `balanced-chat` 验证切换。记录响应模型、完成状态、时延和供应商账单，不把 mock 指标当作真实性能。
