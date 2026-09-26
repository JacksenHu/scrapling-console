# -*- coding: utf-8 -*-
"""
Scrapling 控制台 MCP Server（V7：用户 API Key 鉴权 + 高级工具）
把整个 Web 控制台（代理池/存活池/代理访问/目标站体检/爬虫任务/会话/Scrapling 抓取/高级工具）暴露成 MCP 工具。
鉴权：客户端连接时带 Authorization: Bearer <用户 API Key 或平台主令牌>。
"""
import json
import os
import urllib.parse
import urllib.request
import urllib.error

from fastmcp import FastMCP
from fastmcp.server.auth import AuthProvider, AccessToken
from fastmcp.server.dependencies import get_access_token

import auth

# ---------- 控制台 REST ----------
BASE = "http://127.0.0.1:8080"
# 平台主口令：必须通过环境变量 SCRAPLING_WEB_PIN 注入（勿硬编码进仓库）
PIN = os.environ.get("SCRAPLING_WEB_PIN", "")

# 平台级主令牌（超级通道，等同于 PIN；从环境变量读取，勿硬编码）
# 平台级 MCP 主令牌：必须通过环境变量 SCRAPLING_CONSOLE_MCP_TOKEN 注入（勿硬编码进仓库）
MCP_MASTER_TOKEN = os.environ.get("SCRAPLING_CONSOLE_MCP_TOKEN", "")


def _call(method, path, body=None, timeout=180):
    """转发到控制台 REST。优先带用户 API Key（按用户扣额度），否则走主通道。"""
    headers = {"Content-Type": "application/json"}
    tok = None
    try:
        at = get_access_token()
        if at is not None:
            tok = getattr(at, "token", None) or getattr(at, "access_token", None)
    except Exception:
        tok = None
    if tok and tok != MCP_MASTER_TOKEN:
        headers["X-Api-Key"] = tok
    else:
        headers["X-Auth"] = PIN
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            try:
                return json.loads(raw)
            except Exception:
                return {"raw": raw[:4000]}
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8", "replace")[:500]
        except Exception:
            detail = str(e)
        return {"error": f"HTTP {e.code}: {detail}"}
    except Exception as e:
        return {"error": f"连接失败: {e}"}


def _snippet(obj, n=3000):
    s = json.dumps(obj, ensure_ascii=False)
    return s if len(s) <= n else s[:n] + f"...（已截断，共 {len(s)} 字符）"


# 鉴权：Bearer = 用户 API Key（查询 auth.db）或平台主令牌
class SimpleAuth(AuthProvider):
    async def verify_token(self, token: str):
        if token == MCP_MASTER_TOKEN:
            return AccessToken(token=token, client_id="master", scopes=[])
        user = auth.get_user_by_api_key(token)
        if user and user.get("status") == "active":
            return AccessToken(token=token, client_id=user["id"], scopes=[])
        return None


mcp = FastMCP(
    "Scrapling 云抓取平台",
    instructions=(
        "Scrapling 云抓取平台的 MCP 接口：可直接抓网页、把网页转成 LLM 就绪 Markdown、全站转 Markdown、"
        "捕获页面 API 请求（XHR）、生成/验证选择器、CLI 式提取；用代理池的 IP 访问目标网址（注册页防风控）、"
        "存活检测、目标站代理体检、创建/管理爬虫任务、管理会话。"
        "代理访问场景优先用 proxy_visit（支持 plain/browser/screenshot 三模式与自动换 IP）。"
        "高级工具优先用 page_markdown / site_markdown / capture_xhr / selector_gen / extract_cli。"
    ),
    auth=SimpleAuth(),
)


# =============== 1. 控制台状态 ===============
@mcp.tool()
def console_status() -> str:
    """查看控制台整体状态：MCP 抓取服务是否在线、代理池/存活池/任务数量"""
    d = _call("GET", "/api/status", timeout=30)
    return _snippet(d)


# =============== 2. Scrapling 抓取 ===============
@mcp.tool()
def scrapling_tools() -> str:
    """列出 Scrapling 抓取服务提供的全部工具（fetch/stealthy_fetch/make_request/screenshot/会话等）"""
    d = _call("GET", "/api/mcp/tools", timeout=30)
    return _snippet(d)


@mcp.tool()
def scrapling_call(name: str, arguments: str = "{}") -> str:
    """通用调用 Scrapling 抓取工具。
    name: 工具名（如 fetch、stealthy_fetch、make_request、bulk_get、screenshot、open_session、session_fetch 等，先用 scrapling_tools 查全量）。
    arguments: 工具参数的 JSON 字符串，例如 {"url":"https://example.com","timeout":30000}。
    """
    try:
        args = json.loads(arguments) if arguments and arguments.strip() else {}
    except Exception:
        return 'arguments 必须是合法 JSON，例如 {"url": "https://example.com"}'
    d = _call("POST", "/api/mcp/call", {"name": name, "arguments": args}, timeout=300)
    return _snippet(d)


# =============== 2.5 高级工具（上游 Scrapling 能力补齐） ===============
@mcp.tool()
def page_markdown(url: str, css_selector: str = "") -> str:
    """把任意网页转成 LLM 就绪的干净 Markdown（RAG-ready，自动剥离脚本/广告/导航噪声）。
    url: 目标网址。css_selector: 可选，只提取匹配该 CSS 选择器的内容再转 Markdown。
    返回：markdown 文本（前 2 万字符）。"""
    d = _call("POST", "/api/scrapling/advanced",
              {"tool": "page_markdown", "args": {"url": url, "css_selector": css_selector}}, timeout=300)
    return _snippet(d)


@mcp.tool()
def site_markdown(url: str, max_pages: int = 20) -> str:
    """把整个网站爬成 Markdown 语料库（SiteToMarkdownSpider，自动跟随站内链接）。
    url: 网站入口。max_pages: 最多抓取页数（默认 20）。
    返回：页数与各页 Markdown 摘要。"""
    d = _call("POST", "/api/scrapling/advanced",
              {"tool": "site_markdown", "args": {"url": url, "max_pages": max_pages}}, timeout=300)
    return _snippet(d)


@mcp.tool()
def capture_xhr(url: str, timeout: int = 90) -> str:
    """用浏览器加载页面，自动捕获页面发出的所有 XHR/fetch 请求（API 数据接口），
    无需逆向前端 JS 即可拿到站点背后的 API 地址列表。
    url: 目标网址。返回：捕获到的请求 URL/状态/类型 列表。"""
    d = _call("POST", "/api/scrapling/advanced",
              {"tool": "capture_xhr", "args": {"url": url, "timeout": timeout}}, timeout=300)
    return _snippet(d)


@mcp.tool()
def selector_gen(url: str, css: str = "", xpath: str = "", text: str = "") -> str:
    """在网页上验证/生成选择器：给定 CSS/XPath/文本，返回匹配元素数量与前几个元素的摘要。
    url: 目标网址。css/xpath/text: 三选一。用于确认抓取规则是否有效。"""
    d = _call("POST", "/api/scrapling/advanced",
              {"tool": "selector_gen", "args": {"url": url, "css": css, "xpath": xpath, "text": text}}, timeout=120)
    return _snippet(d)


@mcp.tool()
def extract_cli(url: str, output_format: str = "md", css_selector: str = "",
                mode: str = "get", impersonate: str = "chrome") -> str:
    """CLI extract 命令等价物：一键把网页提取为指定格式内容。
    url: 目标网址。output_format: md=Markdown / txt=纯文本 / html=HTML。
    css_selector: 可选，只提取匹配该选择器的内容。mode: get=普通请求 / fetch=动态渲染 / stealthy-fetch=反爬绕过。
    """
    d = _call("POST", "/api/scrapling/advanced",
              {"tool": "extract_cli", "args": {"url": url, "format": output_format,
                                               "css_selector": css_selector, "mode": mode,
                                               "impersonate": impersonate}}, timeout=300)
    return _snippet(d)


# =============== 3. 代理池 ===============
@mcp.tool()
def proxy_list(q: str = "", source: str = "", limit: int = 50, offset: int = 0) -> str:
    """查询代理池列表。q: 按 IP/端口/协议过滤；source: 按来源过滤；limit/offset: 分页。"""
    path = f"/api/proxies?q={urllib.parse.quote(q)}&source={urllib.parse.quote(source)}&limit={limit}&offset={offset}"
    d = _call("GET", path, timeout=30)
    return _snippet(d)


@mcp.tool()
def proxy_check_alive(limit: int = 100) -> str:
    """对代理池做一次存活检测（默认前 100 条，约 10-60 秒），通过的代理自动存入「存活代理池」。用 proxy_check_alive_status 查进度。"""
    d = _call("GET", f"/api/proxies/check?limit={min(limit, 300)}", timeout=30)
    return _snippet(d)


@mcp.tool()
def proxy_check_alive_status() -> str:
    """查询存活检测进度与结果（running/progress/result）"""
    d = _call("GET", "/api/proxies/check/status", timeout=30)
    return _snippet(d)


@mcp.tool()
def proxy_alive_pool(limit: int = 100) -> str:
    """查询「存活代理池」（存活检测通过、自动入库的代理，访问时自动优先使用）"""
    d = _call("GET", f"/api/proxies/alive?limit={limit}", timeout=30)
    return _snippet(d)


@mcp.tool()
def proxy_alive_remove(ip: str, port: str) -> str:
    """从存活代理池移除单条代理。ip/port: 如 ip='1.1.189.58' port='8080'"""
    d = _call("POST", "/api/proxies/alive/remove", {"ip": ip, "port": port}, timeout=30)
    return _snippet(d)


@mcp.tool()
def proxy_alive_clear() -> str:
    """清空整个存活代理池"""
    d = _call("POST", "/api/proxies/alive/clear", {}, timeout=30)
    return _snippet(d)


# =============== 4. 用代理访问网址（核心：注册场景） ===============
@mcp.tool()
def proxy_visit(url: str, mode: str = "browser", proxy: str = "", retries: int = 2,
                blocked_domains: str = "") -> str:
    """用代理池的 IP 访问一个网址（注册页防风控核心工具）。
    url: 目标网址（如 https://example.com/register）。
    mode: browser=隐身浏览器渲染（推荐注册页）/ screenshot=浏览器截图 / plain=普通HTTP请求（最快）。
    proxy: 可选指定代理（如 1.2.3.4:8080），不填自动优先用「该站可用池→存活池→全池」。
    retries: 失败自动换 IP 重试次数（1-5）。
    blocked_domains: 可选，浏览器加载时拦截的域名（逗号分隔），可拦截广告/统计域。
    返回：出口 IP（保证≠服务器 IP，防泄漏）、耗时、页面内容/截图。
    """
    body = {"url": url, "mode": mode, "proxy": proxy, "retries": retries,
            "blocked_domains": blocked_domains}
    d = _call("POST", "/api/proxy/visit", body, timeout=300)
    return _snippet(d)


@mcp.tool()
def proxy_records(tail: int = 20) -> str:
    """查看最近的代理访问记录（时间/网址/模式/代理/出口IP/状态/耗时）"""
    d = _call("GET", f"/api/proxy/records?tail={tail}", timeout=30)
    return _snippet(d)


# =============== 5. 目标站代理体检 ===============
@mcp.tool()
def proxy_check_target(url: str, limit: int = 100) -> str:
    """目标站代理体检：让代理池挨个访问目标网址，筛出「能打开这个站」的代理（注册专用池）。用 proxy_check_target_status 查进度，proxy_target_usable 查结果。"""
    d = _call("POST", "/api/proxy/check-target", {"url": url, "limit": min(limit, 300)}, timeout=30)
    return _snippet(d)


@mcp.tool()
def proxy_check_target_status() -> str:
    """查询目标站体检进度"""
    d = _call("GET", "/api/proxy/check-target/status", timeout=30)
    return _snippet(d)


@mcp.tool()
def proxy_target_usable(url: str) -> str:
    """查询某目标站的「可用代理池」（体检筛出的、能打开该站的代理）"""
    d = _call("GET", f"/api/proxy/target-usable?url={urllib.parse.quote(url)}", timeout=30)
    return _snippet(d)


# =============== 6. 爬虫任务 ===============
@mcp.tool()
def tasks_list() -> str:
    """查看全部爬虫任务（状态/已抓条数/日志大小）"""
    d = _call("GET", "/api/tasks", timeout=30)
    return _snippet(d)


@mcp.tool()
def task_create(type: str = "crawl", name: str = "", start_urls: str = "",
                target_website: str = "", fields: str = "", allowed_domains: str = "",
                proxy_rotation: bool = False, export_format: str = "jsonl",
                concurrency: int = 4, download_delay: float = 0.0) -> str:
    """创建爬虫任务。
    type: crawl=全站爬取 / single=单页提取 / sitemap=站点地图 / xml=RSS / csv=CSV Feed / shopify=Shopify 产品 / markdown=全站转 Markdown。
    start_urls: 起始网址，逗号或换行分隔。
    target_website: 仅 shopify 任务用（商店域名）。
    fields: 提取字段，每行一个「字段名:选择器」，如「标题:h1\n价格:.price」。
    allowed_domains: 允许爬取的域名（逗号分隔），不填自动取起始网址域名。
    proxy_rotation: 是否轮换代理池。export_format: jsonl/csv/json/xml。
    """
    body = {
        "type": type, "name": name or "", "start_urls": start_urls,
        "target_website": target_website, "fields": fields,
        "allowed_domains": allowed_domains, "proxy_rotation": proxy_rotation,
        "export_format": export_format, "concurrency": concurrency,
        "download_delay": download_delay,
    }
    d = _call("POST", "/api/tasks", body, timeout=60)
    return _snippet(d)


@mcp.tool()
def task_start(tid: str) -> str:
    """启动一个爬虫任务（tid: 任务 ID，如 t260925143000abcd）"""
    d = _call("POST", f"/api/tasks/{tid}/start", {}, timeout=60)
    return _snippet(d)


@mcp.tool()
def task_pause(tid: str) -> str:
    """暂停一个运行中的爬虫任务"""
    d = _call("POST", f"/api/tasks/{tid}/pause", {}, timeout=60)
    return _snippet(d)


@mcp.tool()
def task_resume(tid: str) -> str:
    """恢复一个已暂停的爬虫任务"""
    d = _call("POST", f"/api/tasks/{tid}/resume", {}, timeout=60)
    return _snippet(d)


@mcp.tool()
def task_log(tid: str, lines: int = 100) -> str:
    """查看爬虫任务运行日志（lines: 最近行数）"""
    d = _call("GET", f"/api/tasks/{tid}/log?lines={lines}", timeout=30)
    return _snippet(d)


# =============== 7. 会话 ===============
@mcp.tool()
def sessions_list() -> str:
    """查看当前打开的浏览器/请求会话"""
    d = _call("GET", "/api/sessions", timeout=30)
    return _snippet(d)


@mcp.tool()
def session_close(sid: str) -> str:
    """关闭一个会话（sid: 会话 ID）"""
    d = _call("POST", f"/api/sessions/{sid}/close", {}, timeout=30)
    return _snippet(d)


if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8100)
