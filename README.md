# Scrapling Console —— 现代网页的无痛抓取平台

> Effortless Web Scraping for the Modern Web
> 基于开源 
>
> [Scrapling](https://github.com/D4Vinci/Scrapling)
>
>  构建的 SaaS 化网页抓取控制台：网页抓取、反爬绕过、Markdown 转换、代理池访问、MCP / REST API —— 一个平台全搞定。



![stack](https://img.shields.io/badge/FastAPI-3.0-009688)



![stack](https://img.shields.io/badge/FastMCP-4.0-2dd4bf)



![stack](https://img.shields.io/badge/SQLite-标准库-0a1628)



![license](https://img.shields.io/badge/License-BSD--3--Clause-blue)



***

## 为什么用 Scrapling Console

从单次请求到全站爬取，覆盖你 90% 的抓取需求 —— 全部通过**网页控制台**、**REST API** 或 **MCP（给 AI agent 用）** 三种方式调用。



* 🛡️ **反爬绕过，开箱即用**：底层 StealthyFetcher 模拟真人指纹与行为，开箱绕过 Cloudflare Turnstile 等人机验证；DynamicFetcher 渲染动态页面。

* 🧠 **自适应选择器**：解析器会学习网站变化，`adaptive=True` 自动重新定位元素 —— 网站改版，抓取规则依然生效。

* 🌐 **代理池访问（注册防风控）**：用代理池的 IP 打开注册页 / 目标站，自动换 IP、出口 IP 防泄漏，不再担心本人 IP 频繁注册触发风控。

* 📄 **页面转 LLM 就绪 Markdown**：一行 `page.markdown()`，把任意网页变成干净、脱敏、可直接喂给大模型的 Markdown。

* 🗂️ **全站转 Markdown 语料库**：`SiteToMarkdownSpider` 把整个网站爬成 Markdown 语料，用于 RAG / 知识库。

* 📡 **后台 API 捕获**：`capture_xhr` 加载页面并收集它发出的全部 XHR/fetch 请求，站点背后的数据接口一目了然。

* 🕸️ **全站爬取框架**：并发、暂停 / 恢复（断点续爬）、流式输出、自动限速（AutoThrottle）、robots.txt 合规、代理轮换、开发模式缓存。



***

## 功能总览

### 🖥️ Web 控制台（登录后）



| 模块     | 说明                                                                       |
| ------ | ------------------------------------------------------------------------ |
| 概览     | 余额、额度用量、快速开始、系统状态                                                        |
| 抓取工具   | MCP 工具台：填 URL 一键抓取（普通 / 反爬 / 截图 / 纯请求 / 批量 / 提取链接）+ 自适应选择器 + 代理轮换 + 会话管理 |
| 高级工具   | 页面转 Markdown / 全站转 Markdown / 后台 API 捕获 / 选择器生成与验证 /extract-cli          |
| 代理池・访问 | 代理池搜索与存活检测、🟢 存活代理池、用代理打开网址（自动换 IP + 出口防泄漏）、目标站代理体检、访问记录                 |
| 爬虫任务   | 全站爬取 / 单页提取 / Sitemap / Feed / Shopify 等 6 种任务，后台运行、暂停恢复、四种格式导出          |
| 教程     | 小白向完整教程 + SVG 决策导图                                                       |
| API 接入 | 生成 / 管理 API Key（供应用接入）                                                   |
| 额度与购买  | 余额、用量明细；未配置支付时显示「联系客服」                                                   |
| 服务状态   | MCP / 工具 / 代理池 / 系统资源                                                    |
| 管理后台   | 平台统计、用户管理（开号 / 充值 / 禁用 / 角色）、站点设置、发信邮箱 SMTP 配置与测试                        |

### 🔌 REST API（消耗 token）

所有接口支持三种鉴权头之一：`X-Auth: <主口令>` / `X-Session-Token: <登录会话>` / `X-Api-Key: <用户 API Key>`。消耗型接口每次扣 1 额度（额度不足返回 402）。



```
\# 抓取调用示例

curl -X POST https://YOUR-SERVER:8080/api/mcp/call \\

&#x20; -H "X-Api-Key: sk-xxx" -H "Content-Type: application/json" \\

&#x20; -d '{"name":"scrapling\_call","arguments":{"action":"fetch","url":"https://example.com"}}'

\# 用代理访问目标站（注册防风控核心）

curl -X POST https://YOUR-SERVER:8080/api/proxy/visit \\

&#x20; -H "X-Api-Key: sk-xxx" -H "Content-Type: application/json" \\

&#x20; -d '{"url":"https://example.com/register","mode":"browser","retries":3}'
```

### 🤖 MCP Server（27 个工具，给 AI agent 用）

入口：`https://YOUR-SERVER:8080/mcp/`，Bearer 鉴权（用户 API Key 或平台主令牌）。



* 状态：`console_status`

* 抓取：`scrapling_tools` / `scrapling_call` / `page_markdown` / `site_markdown` / `capture_xhr` / `selector_gen` / `extract_cli`

* 代理池：`proxy_list` / `proxy_check_alive` / `proxy_check_alive_status` / `proxy_alive_pool` / `proxy_alive_remove` / `proxy_alive_clear`

* 代理访问：`proxy_visit`（plain/browser/screenshot + 自动换 IP + 出口防泄漏）/ `proxy_records` / `proxy_check_target` / `proxy_check_target_status` / `proxy_target_usable`

* 爬虫任务：`tasks_list` / `task_create` / `task_start` / `task_pause` / `task_resume` / `task_log`

* 会话：`sessions_list` / `session_close`

**Claude Desktop 配置示例**：



```
{

&#x20; "mcpServers": {

&#x20;   "scrapling-console": {

&#x20;     "type": "http",

&#x20;     "url": "https://YOUR-SERVER:8080/mcp/",

&#x20;     "headers": { "Authorization": "Bearer sk-你的APIKey" }

&#x20;   }

&#x20; }

}
```

### 📧 邮箱注册登录 + 发信邮箱（SMTP）



* 邮箱即账号：注册 / 登录使用邮箱 + 密码，注册即送试用额度（`trial_quota` 可配）

* 管理后台可配置 SMTP（服务器 / 端口 / 账号 / 授权码 / 发件人 / SSL 或 STARTTLS），新用户注册自动收到欢迎邮件，支持一键「发送测试」



***

## 快速部署

### 环境要求



* Python 3.11+（Web 服务）

* Docker（可选：爬虫任务 / 高级工具在 [pyd4vinci/scrapling](https://hub.docker.com/r/pyd4vinci/scrapling) 容器内执行）

### 步骤



```
\# 1. 安装依赖

pip install fastapi uvicorn fastmcp

\# 2. 设置环境变量（务必覆盖默认值）

export SCRAPLING\_WEB\_PIN="你的平台主口令"              # Web/API 主口令（勿用默认空值）

export SCRAPLING\_CONSOLE\_MCP\_TOKEN="你的MCP主令牌"     # MCP 平台级 Bearer 令牌

export SCRAPLING\_MCP\_AUTH\_TOKEN="内部抓取容器令牌"      # 容器 MCP 上游鉴权

export SCRAPLING\_AUTH\_DB="/opt/scrapling/app.db"      # SQLite 路径

\# 3. 启动

uvicorn app:app --host 0.0.0.0 --port 8080
```

> 生产环境建议用 systemd 或 Docker 管理进程；数据库首次启动自动建表（users /sessions/api_keys /usage_log/settings）。

### 文件说明



| 文件                  | 职责                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------ |
| `app.py`            | FastAPI 主应用：多通道鉴权中间件、用户 / 管理员 / 额度 / 高级工具 API、MCP 挂载                                 |
| `auth.py`           | SQLite 认证与额度系统：用户 / 会话 / API Key / 用量 / SMTP 发信                                      |
| `mcp_server.py`     | FastMCP Server：27 个工具，Bearer 鉴权（用户 API Key 或主令牌）                                     |
| `advanced_tools.py` | 高级工具执行器（容器内运行 page.markdown/ SiteToMarkdownSpider /capture\_xhr/ 选择器生成 /extract-cli） |
| `index.html`        | 单文件前端：首页 / 登录注册 / 控制台 10 模块 / 管理后台                                                   |
| `spider_gen.py`     | 爬虫任务 Spider 脚本生成器                                                                    |
| `quick_tool.py`     | 快捷抓取执行器（自适应 / 代理轮换 / 缓存 / 链接提取）                                                      |



***

## 上游项目



* 官网文档：[https://scrapling.readthedocs.io/](https://scrapling.readthedocs.io/)

* GitHub：[https://github.com/D4Vinci/Scrapling](https://github.com/D4Vinci/Scrapling)

* 许可：本仓库遵循上游 BSD-3-Clause 协议，使用请保留署名。

## License

BSD-3-Clause