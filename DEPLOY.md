# 部署指南（公开版）

> 完整内部部署记录见项目内部文档；本文件为可公开的部署指南，所有密钥均用环境变量注入。

## 1. 部署架构

| 组件 | 说明 |
| --- | --- |
| Web 服务 | FastAPI + uvicorn，端口 8080，systemd 管理，开机自启 |
| MCP Server | FastMCP Streamable HTTP，挂载于 `/mcp/`，Bearer 鉴权 |
| 数据库 | SQLite（users / sessions / api_keys / usage_log / settings），WAL 模式 |
| 爬虫执行 | `pyd4vinci/scrapling` Docker 镜像（含 Playwright），容器内运行任务/高级工具 |

## 2. 首次部署步骤

```bash
# 安装依赖
pip install fastapi uvicorn fastmcp

# 环境变量（必设，勿硬编码到代码/仓库）
export SCRAPLING_WEB_PIN="<你的平台主口令>"          # 管理员万能通道
export SCRAPLING_CONSOLE_MCP_TOKEN="<你的MCP主令牌>" # MCP 平台级 Bearer
export SCRAPLING_MCP_AUTH_TOKEN="<抓取容器令牌>"     # 容器 MCP 上游鉴权
export SCRAPLING_AUTH_DB="<sqlite路径>"

# 启动（生产建议 systemd）
uvicorn app:app --host 0.0.0.0 --port 8080
```

## 3. 验证清单

- [ ] 注册邮箱用户 → 返回 token，/api/me 额度 = trial_quota
- [ ] 未授权访问 /api/proxies → 401（JSONResponse，不是 500）
- [ ] 额度不足调用消耗接口 → 402「额度不足」
- [ ] 管理员（X-Auth 主口令）访问 /api/admin/* → 200；普通用户 → 403
- [ ] MCP：initialize → tools/list 27 工具；无效 Bearer → 401
- [ ] 管理后台配置 SMTP 后「发送测试」→ 收件箱收到测试邮件

## 4. 常见问题

- **中间件 401 变 500**：Starlette BaseHTTPMiddleware 内不能 `raise HTTPException`，必须 `return JSONResponse`。
- **FastMCP 挂载 307/路径叠加**：用 `mcp.http_app(path="/")` 并合并 lifespan。
- **MCP Missing Session ID**：Streamable HTTP 需先 initialize 拿 `Mcp-Session-Id` 再 tools/list。
- **免费代理存活率低**：建议配合付费/自有代理，或对目标站做代理体检后趁窗口期使用。
