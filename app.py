"""
Scrapling Web Console - 单应用控制台
功能：MCP 工具操作台 + 代理池管理 + 服务状态
"""
import sys
import json
import os
import re
import csv
import io
import random
import subprocess
import time
import uuid
import threading
import concurrent.futures
import urllib.request

from urllib.parse import quote
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from spider_gen import gen_spider_script
import auth

auth.init_db()

MCP_URL = "http://127.0.0.1:8000/mcp"
MCP_TOKEN = os.environ.get("SCRAPLING_MCP_AUTH_TOKEN", "")
# 平台主口令：必须通过环境变量 SCRAPLING_WEB_PIN 注入（勿硬编码进仓库）
WEB_PIN = os.environ.get("SCRAPLING_WEB_PIN", "")
PROXY_JSON = "/opt/scrapling/proxies.json"
PROXY_CSV = "/opt/scrapling/proxies.csv"
PROXY_SCRIPT = "/opt/scrapling/proxy_scraper.py"

app = FastAPI(title="Scrapling Web Console")

# ---------------- 鉴权（PIN 万能通道 / API Key / 会话 Token） ----------------

AUTH_FREE_PREFIXES = ("/api/ping", "/api/auth/", "/api/settings-public", "/api/stats-public", "/mcp")

MASTER_USER = {"id": "master", "username": "master", "role": "master",
               "quota": 999999999, "total_quota": 999999999, "status": "active"}

def check_pin(request: Request):
    pin = request.headers.get("X-Auth", "")
    if pin != WEB_PIN:
        raise HTTPException(status_code=401, detail="未授权")

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/") and not any(path.startswith(p) for p in AUTH_FREE_PREFIXES):
        user = None
        # 1) 旧 PIN 万能通道（平台主人，拥有全部权限与无限额度）
        pin = request.headers.get("X-Auth", "")
        if pin and pin == WEB_PIN:
            user = MASTER_USER
        else:
            # 2) API Key（应用程序接入）
            ak = request.headers.get("X-Api-Key", "")
            if ak:
                user = auth.get_user_by_api_key(ak)
                if not user:
                    return JSONResponse(status_code=401, content={"detail": "API Key 无效"})
            else:
                # 3) 会话 Token（前端登录）
                st = request.headers.get("X-Session-Token", "")
                if st:
                    user = auth.get_session_user(st)
                    if not user:
                        return JSONResponse(status_code=401, content={"detail": "登录已失效，请重新登录"})
                # 4) 应用令牌（第三方应用静默上报住宅节点通道，仅限住宅相关端点）
                if not user:
                    at = request.headers.get("X-App-Token", "")
                    if at:
                        app = next((a for a in load_residential_apps() if a.get("token") == at), None)
                        if app:
                            user = {"id": "app:" + str(app.get("id")),
                                    "username": "app:" + str(app.get("name", "")),
                                    "role": "user", "quota": 0, "total_quota": 0,
                                    "status": "active", "_app": app}
                        else:
                            return JSONResponse(status_code=401, content={"detail": "App Token 无效"})
        if not user:
            return JSONResponse(status_code=401, content={"detail": "未授权"})
        if user.get("status") == "disabled":
            return JSONResponse(status_code=403, content={"detail": "账号已被禁用"})
        request.state.user = user
    try:
        return await call_next(request)
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"detail": e.detail})

def current_user(request: Request) -> dict:
    return getattr(request.state, "user", None) or MASTER_USER

def require_quota(request: Request, cost: int = 1) -> dict:
    """校验额度并扣减；master（PIN 通道）不扣"""
    user = current_user(request)
    if user.get("role") == "master":
        return user
    if not auth.consume_quota(user["id"], cost):
        raise HTTPException(status_code=402, detail="额度不足，请先购买额度或联系客服充值")
    user["quota"] = user.get("quota", 0) - cost
    return user

def require_admin(request: Request) -> dict:
    user = current_user(request)
    if user.get("role") not in ("admin", "master"):
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user

# ---------------- MCP 客户端 ----------------

mcp_session = requests.Session()
_mcp_session_id = {"id": None}
_mcp_lock = threading.Lock()

def mcp_request(method: str, params: dict = None):
    """调用 MCP JSON-RPC，自动处理 initialize + 会话"""
    with _mcp_lock:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if MCP_TOKEN:
            headers["Authorization"] = f"Bearer {MCP_TOKEN}"
        sid = _mcp_session_id["id"]
        if sid:
            headers["Mcp-Session-Id"] = sid

        # 初始化（首次或会话丢失）
        if method != "initialize" and not sid:
            r = mcp_session.post(MCP_URL, json={
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-03-26",
                           "capabilities": {},
                           "clientInfo": {"name": "web-console", "version": "1.0"}}},
                headers=headers, timeout=30)
            body = parse_mcp_response(r)
            new_sid = r.headers.get("Mcp-Session-Id")
            if new_sid:
                _mcp_session_id["id"] = new_sid
                headers["Mcp-Session-Id"] = new_sid
            _ = body  # initialize result

        req = {"jsonrpc": "2.0", "id": 2, "method": method}
        if params is not None:
            req["params"] = params
        r = mcp_session.post(MCP_URL, json=req, headers=headers, timeout=300)
        return parse_mcp_response(r), r.headers.get("Mcp-Session-Id")

def parse_mcp_response(r):
    """解析 Streamable HTTP 响应（可能是 SSE 或纯 JSON）"""
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"MCP 上游错误 {r.status_code}: {r.text[:300]}")
    ctype = r.headers.get("Content-Type", "")
    if "text/event-stream" in ctype:
        result = None
        error = None
        for line in r.text.splitlines():
            if line.startswith("data:"):
                try:
                    msg = json.loads(line[5:].strip())
                except Exception:
                    continue
                if "result" in msg:
                    result = msg["result"]
                if "error" in msg:
                    error = msg["error"]
        if error:
            raise HTTPException(status_code=502, detail=f"MCP 错误: {json.dumps(error, ensure_ascii=False)}")
        return result
    data = r.json()
    if "error" in data:
        raise HTTPException(status_code=502, detail=f"MCP 错误: {json.dumps(data['error'], ensure_ascii=False)}")
    return data.get("result")

@app.get("/api/ping")
def ping():
    return {"ok": True}

@app.get("/api/status")
def status():
    # MCP 服务状态
    try:
        tools, _ = mcp_request("tools/list")
        mcp_ok = True
        tool_count = len(tools.get("tools", [])) if tools else 0
    except Exception as e:
        mcp_ok = False
        tool_count = 0
        tools = None
    # 代理数据
    proxy_count = 0
    try:
        with open(PROXY_JSON, "r", encoding="utf-8") as f:
            proxy_count = len(json.load(f))
    except Exception:
        pass
    # 系统资源
    sysinfo = {}
    for cmd, key in [
        ("free -h | head -2 | tail -1", "mem"),
        ("df -h / | tail -1", "disk"),
        ("uptime -p", "uptime"),
    ]:
        try:
            out = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10).stdout.strip()
            sysinfo[key] = out
        except Exception:
            sysinfo[key] = ""
    return {
        "mcp": {"ok": mcp_ok, "tools": tool_count},
        "proxies": proxy_count,
        "system": sysinfo,
    }

@app.get("/api/mcp/tools")
def mcp_tools():
    result, _ = mcp_request("tools/list")
    if not result:
        return {"tools": []}
    return {"tools": result.get("tools", [])}

@app.post("/api/mcp/call")
async def mcp_call(request: Request):
    require_quota(request)
    body = await request.json()
    name = body.get("name", "")
    arguments = body.get("arguments", {})
    if not name:
        raise HTTPException(status_code=400, detail="缺少工具名")
    result, _ = mcp_request("tools/call", {"name": name, "arguments": arguments})
    return {"result": result}

@app.post("/api/mcp/session/reset")
def mcp_session_reset():
    _mcp_session_id["id"] = None
    return {"ok": True}

# ---------------- 内置浏览器：真实交互会话（Playwright 驱动） ----------------
_BWS = {}
_BWS_TMP = "/tmp/scrapling-bw"
_BW_MAX = 8
_BW_TTL = 600
os.makedirs(_BWS_TMP, exist_ok=True)

async def _bw_shot(browser_id):
    bws = _BWS.get(browser_id)
    if not bws:
        return None
    page = bws["page"]
    data = await page.screenshot(type="jpeg", quality=60)
    import base64
    return {"shot": "data:image/jpeg;base64," + base64.b64encode(data).decode(),
            "url": page.url, "title": await page.title()}

async def _bw_gc():
    now = time.time()
    for k in list(_BWS.keys()):
        if now - _BWS[k]["ts"] > _BW_TTL:
            try:
                await _BWS[k]["browser"].close()
            except Exception:
                pass
            _BWS.pop(k, None)

@app.post("/api/browser/open")
async def api_browser_open(request: Request):
    require_quota(request)
    body = await request.json()
    url = (body.get("url") or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="请填写网址")
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    proxy = (body.get("proxy") or "").strip()
    if proxy and not proxy.startswith(("http://", "https://", "socks5://")):
        proxy = "http://" + proxy
    await _bw_gc()
    if len(_BWS) >= _BW_MAX:
        old = min(_BWS.keys(), key=lambda k: _BWS[k]["ts"])
        try:
            await _BWS[old]["browser"].close()
        except Exception:
            pass
        _BWS.pop(old, None)
    from playwright.async_api import async_playwright
    p = await async_playwright().start()
    bid = "bw-" + uuid.uuid4().hex[:12]
    launcher = {"headless": True,
                "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"],
                "viewport": {"width": 1280, "height": 900},
                "user_data_dir": os.path.join(_BWS_TMP, bid),
                "locale": "zh-CN"}
    if proxy:
        launcher["proxy"] = {"server": proxy}
    ctx = await p.chromium.launch_persistent_context(**launcher)
    page = ctx.pages[0] if ctx.pages else await ctx.new_page()
    try:
        await page.goto(url, timeout=45000, wait_until="domcontentloaded")
    except Exception:
        pass
    _BWS[bid] = {"browser": p, "ctx": ctx, "page": page, "ts": time.time()}
    shot = await _bw_shot(bid)
    return {"browser_id": bid, "shot": shot["shot"], "url": page.url, "title": shot["title"]}

@app.post("/api/browser/act")
async def api_browser_act(request: Request):
    require_quota(request, cost=0)
    body = await request.json()
    bid = (body.get("browser_id") or "").strip()
    op = body.get("op", "")
    bws = _BWS.get(bid)
    if not bws:
        raise HTTPException(status_code=404, detail="会话不存在或已过期，请重新「启动会话」")
    page = bws["page"]
    try:
        if op == "click":
            x = int(float(body.get("x", 0)) / 1000 * 1280)
            y = int(float(body.get("y", 0)) / 1000 * 900)
            await page.mouse.click(x, y)
        elif op == "type":
            x = int(float(body.get("x", 0)) / 1000 * 1280)
            y = int(float(body.get("y", 0)) / 1000 * 900)
            text = body.get("text", "")
            await page.mouse.click(x, y)
            await page.keyboard.type(text, delay=15)
        elif op == "scroll":
            await page.mouse.wheel(0, int(body.get("dy", 0)))
        elif op == "back":
            await page.go_back()
        elif op == "forward":
            await page.go_forward()
        elif op == "reload":
            await page.reload()
        elif op == "goto":
            u = (body.get("url") or "").strip()
            if u:
                if not u.startswith(("http://", "https://")):
                    u = "https://" + u
                await page.goto(u, timeout=45000, wait_until="domcontentloaded")
        else:
            raise HTTPException(status_code=400, detail="未知操作: " + op)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail="操作失败: " + str(e)[:200])
    bws["ts"] = time.time()
    shot = await _bw_shot(bid)
    return {"shot": shot["shot"], "url": shot["url"], "title": shot["title"]}

@app.post("/api/browser/close")
async def api_browser_close(request: Request):
    body = await request.json()
    bid = (body.get("browser_id") or "").strip()
    bws = _BWS.pop(bid, None)
    if bws:
        try:
            await bws["browser"].close()
        except Exception:
            pass
    return {"ok": True}


# ---------------- 快捷任务编排 ----------------

def call_tool(name: str, arguments: dict):
    """调用 MCP 工具并返回原始结果"""
    result, _ = mcp_request("tools/call", {"name": name, "arguments": arguments})
    return result

def parse_result_content(result):
    """把 MCP 工具返回的 content 转成前端可展示格式"""
    if not result:
        return {"type": "empty", "text": ""}
    if result.get("isError"):
        return {"type": "error", "text": json.dumps(result, ensure_ascii=False, indent=2)}
    content = result.get("content") or []
    texts = []
    images = []
    for c in content:
        t = c.get("type")
        if t == "text" and c.get("text"):
            texts.append(c["text"])
        elif t == "image":
            mime = c.get("mimeType", "image/png")
            data = c.get("data", "")
            images.append(f"data:{mime};base64,{data}")
        elif t == "resource":
            texts.append(f"[资源] {c.get('uri', '')}\n{(c.get('text') or '')[:4000]}")
    if images:
        return {"type": "image", "images": images, "text": "\n".join(texts)}
    return {"type": "text", "text": "\n".join(texts) or json.dumps(result, ensure_ascii=False, indent=2)[:8000]}

@app.post("/api/mcp/quick")
async def mcp_quick(request: Request):
    """快捷任务：填 URL 即可自动编排"""
    require_quota(request)
    body = await request.json()
    mode = body.get("mode", "simple")
    url = (body.get("url") or "").strip()
    urls_raw = body.get("urls") or ""
    css_selector = (body.get("css_selector") or "").strip() or None
    method = body.get("method") or "GET"
    extraction_type = body.get("extraction_type") or "markdown"
    # 增强选项
    adaptive = bool(body.get("adaptive", False))
    use_proxy = bool(body.get("use_proxy", False))
    dev_cache = bool(body.get("dev_cache", False))

    if mode in ("simple", "stealthy", "extract", "screenshot", "make_request", "links"):
        if not url:
            raise HTTPException(status_code=400, detail="请填写 URL")
    if mode in ("bulk", "bulk_stealthy"):
        urls = [u.strip() for u in urls_raw.replace("\n", ",").split(",") if u.strip()]
        if not urls:
            raise HTTPException(status_code=400, detail="请填写 URL 列表（逗号或换行分隔）")

    # 增强模式走容器脚本（MCP 工具不支持 adaptive/代理轮换/缓存）
    if mode == "links" or adaptive or use_proxy or dev_cache:
        try:
            cfg = {
                "action": "links" if mode == "links" else "fetch",
                "url": url,
                "css_selector": css_selector or "",
                "extraction_type": extraction_type,
                "adaptive": adaptive,
                "use_proxy": use_proxy,
                "dev_cache": dev_cache,
                "mode": "stealthy" if mode == "stealthy" else "simple",
                "main_content_only": False,
                "timeout": 30,
            }
            cfg_path = f"/opt/scrapling/.quick_{int(time.time())}_{uuid.uuid4().hex[:6]}.json"
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False)
            r = subprocess.run(
                ["sudo", "docker", "run", "--rm", "--entrypoint", "/app/.venv/bin/python",
                 "-v", "/opt/scrapling:/work", "pyd4vinci/scrapling",
                 "/work/quick_tool.py", f"/work/{os.path.basename(cfg_path)}"],
                capture_output=True, text=True, timeout=180)
            try:
                os.remove(cfg_path)
            except Exception:
                pass
            if r.returncode != 0:
                raise HTTPException(status_code=502, detail=f"脚本执行失败: {r.stderr[:500]}")
            try:
                data = json.loads(r.stdout.strip().splitlines()[-1])
            except Exception:
                raise HTTPException(status_code=502, detail=f"脚本输出异常: {r.stdout[:300]}")
            if mode == "links":
                return {"ok": data.get("ok", False), "mode": mode,
                        "steps": [{"tool": "LinkExtractor(容器)", "args": {"url": url}}],
                        "result": parse_result_content({"content": [
                            {"type": "text", "text": "\n".join(data.get("links", []) or ["(无链接)"])}
                        ]}),
                        "link_count": data.get("link_count", 0),
                        "status": data.get("status")}
            return {"ok": data.get("ok", False), "mode": mode,
                    "steps": [{"tool": "scrapling-容器", "args": cfg}],
                    "result": parse_result_content({"content": [
                        {"type": "text", "text": (data.get("text") or "")[:8000] +
                         (f"\n\n[items {data.get('item_count', 0)}]" if data.get("items") else "")}
                    ]}),
                    "items": data.get("items"),
                    "status": data.get("status"),
                    "error": data.get("error")}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"执行失败: {type(e).__name__}: {e}")

    try:
        if mode == "simple":
            # 普通抓取：Playwright 浏览器渲染，适合低-中防护站点
            args = {"url": url, "extraction_type": extraction_type, "main_content_only": False}
            if css_selector:
                args["css_selector"] = css_selector
            return {"ok": True, "mode": mode,
                    "steps": [{"tool": "fetch", "args": args}],
                    "result": parse_result_content(call_tool("fetch", args))}

        if mode == "extract":
            # 提取指定元素（CSS 选择器）
            args = {"url": url, "extraction_type": extraction_type, "main_content_only": False}
            if css_selector:
                args["css_selector"] = css_selector
            return {"ok": True, "mode": mode,
                    "steps": [{"tool": "fetch", "args": args}],
                    "result": parse_result_content(call_tool("fetch", args))}

        if mode == "stealthy":
            # 反爬抓取：隐身模式，适合高防护站点（Cloudflare 等）
            args = {"url": url, "extraction_type": extraction_type, "main_content_only": False}
            if css_selector:
                args["css_selector"] = css_selector
            return {"ok": True, "mode": mode,
                    "steps": [{"tool": "stealthy_fetch", "args": args}],
                    "result": parse_result_content(call_tool("stealthy_fetch", args))}

        if mode == "bulk":
            return {"ok": True, "mode": mode,
                    "steps": [{"tool": "bulk_fetch", "args": {"urls": urls, "extraction_type": extraction_type}}],
                    "result": parse_result_content(call_tool("bulk_fetch", {"urls": urls, "extraction_type": extraction_type}))}

        if mode == "bulk_stealthy":
            return {"ok": True, "mode": mode,
                    "steps": [{"tool": "bulk_stealthy_fetch", "args": {"urls": urls, "extraction_type": extraction_type}}],
                    "result": parse_result_content(call_tool("bulk_stealthy_fetch", {"urls": urls, "extraction_type": extraction_type}))}

        if mode == "screenshot":
            # 自动编排：开会话 → 截图 → 关会话
            sess = call_tool("open_session", {"session_type": "dynamic", "headless": True})
            session_id = None
            if isinstance(sess, dict):
                session_id = sess.get("session_id") or (sess.get("output") or {}).get("session_id")
                if not session_id:
                    session_id = sess.get("session_id") or sess.get("id")
            if not session_id:
                session_id = str(sess)
            shot = call_tool("screenshot", {"url": url, "session_id": str(session_id), "image_type": "png", "full_page": True})
            try:
                call_tool("close_session", {"session_id": str(session_id)})
            except Exception:
                pass
            return {"ok": True, "mode": mode,
                    "steps": [{"tool": "open_session"}, {"tool": "screenshot"}, {"tool": "close_session"}],
                    "result": parse_result_content(shot)}

        if mode == "make_request":
            args = {"url": url, "method": method}
            return {"ok": True, "mode": mode,
                    "steps": [{"tool": "make_request", "args": args}],
                    "result": parse_result_content(call_tool("make_request", args))}

        raise HTTPException(status_code=400, detail=f"未知模式: {mode}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"执行失败: {type(e).__name__}: {e}")

# ---------------- 代理池 ----------------

def load_proxies():
    try:
        with open(PROXY_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

@app.get("/api/proxies")
def api_proxies(q: str = "", source: str = "", limit: int = 200, offset: int = 0):
    all_p = load_proxies()
    if q:
        ql = q.lower()
        all_p = [p for p in all_p if ql in p["ip"] or ql in p["port"] or ql in p.get("protocol", "")]
    if source:
        all_p = [p for p in all_p if p.get("source") == source]
    total = len(all_p)
    page = all_p[offset:offset + limit]
    return {"total": total, "items": page, "offset": offset, "limit": limit}

@app.get("/api/proxies/sources")
def proxy_sources():
    seen = set()
    for p in load_proxies():
        seen.add(p.get("source", "unknown"))
    return {"sources": sorted(seen)}

_check_status = {"running": False, "progress": {"total": 0, "done": 0, "ok": 0}, "result": None}

@app.get("/api/proxies/check")
def proxy_check(request: Request, limit: int = 100):
    """并发存活检测，用代理请求百度首页"""
    require_quota(request)
    if _check_status["running"]:
        return {"running": True, **_check_status["progress"]}
    proxies = load_proxies()[:limit]
    if not proxies:
        return {"running": False, "result": []}

    def check_one(pr):
        ip, port = pr["ip"], pr["port"]
        proto = pr.get("protocol", "http")
        proxy = f"http://{ip}:{port}"
        try:
            r = requests.get("https://www.baidu.com", proxies={"http": proxy, "https": proxy},
                             timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            return {"ip": ip, "port": port, "protocol": proto, "source": pr.get("source", ""),
                    "alive": r.status_code == 200, "code": r.status_code, "ms": int(r.elapsed.total_seconds() * 1000)}
        except Exception:
            return {"ip": ip, "port": port, "protocol": proto, "source": pr.get("source", ""),
                    "alive": False, "code": 0, "ms": 0}

    _check_status["running"] = True
    _check_status["progress"] = {"total": len(proxies), "done": 0, "ok": 0}
    _check_status["result"] = None

    def run():
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
            for res in ex.map(check_one, proxies):
                results.append(res)
                _check_status["progress"]["done"] += 1
                if res["alive"]:
                    _check_status["progress"]["ok"] += 1
        results.sort(key=lambda x: (not x["alive"], x["ms"]))
        alive = [r for r in results if r["alive"]]
        # 存活检测通过 → 自动写入「存活代理池」
        try:
            cur, _ = load_alive()
            for r in alive:
                cur = [it for it in cur if not (it.get("ip") == r["ip"] and str(it.get("port")) == str(r["port"]))]
                cur.append({"ip": r["ip"], "port": str(r["port"]), "protocol": r.get("protocol", "http"),
                            "source": r.get("source", ""), "ms": r.get("ms", 0), "exit_ip": "",
                            "checked_at": time.strftime("%Y-%m-%d %H:%M:%S")})
            save_alive(cur)
        except Exception:
            pass
        _check_status["result"] = {"checked": len(results), "alive": len(alive), "dead": len(results) - len(alive), "items": results[:200]}
        _check_status["running"] = False

    threading.Thread(target=run, daemon=True).start()
    return {"running": True, "total": len(proxies)}

@app.get("/api/proxies/check/status")
def proxy_check_status():
    return {"running": _check_status["running"], "progress": _check_status["progress"], "result": _check_status["result"]}

@app.get("/api/proxies/alive")
def proxy_alive(limit: int = 200):
    """存活代理池（存活检测通过的代理，访问时自动优先使用）"""
    items, updated_at = load_alive()
    return {"updated_at": updated_at, "total": len(items), "items": items[:limit]}


@app.post("/api/proxies/alive/clear")
async def proxy_alive_clear():
    """清空存活代理池"""
    save_alive([], time.strftime("%Y-%m-%d %H:%M:%S"))
    return {"ok": True}


@app.post("/api/proxies/alive/remove")
async def proxy_alive_remove(request: Request):
    """移除存活池中的单条代理"""
    body = await request.json()
    ip, port = (body.get("ip") or "").strip(), str(body.get("port") or "").strip()
    cur, _ = load_alive()
    cur = [it for it in cur if not (it.get("ip") == ip and str(it.get("port")) == port)]
    save_alive(cur)
    return {"ok": True, "left": len(cur)}
@app.post("/api/proxies/alive/add")
async def proxy_alive_add(request: Request):
    """手动添加一条代理到存活池（默认立即验证，verify=false 仅入库）"""
    body = await request.json()
    ip = (body.get("ip") or "").strip()
    port = str(body.get("port") or "").strip()
    protocol = (body.get("protocol") or "http").strip().lower() or "http"
    verify = body.get("verify", True)
    if not ip or not port.isdigit():
        raise HTTPException(status_code=400, detail="请填写有效的 IP 和端口（格式 ip:port）")
    item = {"ip": ip, "port": port, "protocol": protocol, "ms": None, "exit_ip": None,
            "checked_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    if verify:
        proxy = f"{protocol}://{ip}:{port}"
        try:
            r = requests.get("https://www.baidu.com", proxies={"http": proxy, "https": proxy},
                             timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                raise Exception(f"HTTP {r.status_code}")
            item["ms"] = int(r.elapsed.total_seconds() * 1000)
            try:
                er = requests.get(ECHO_SERVICE, proxies={"http": proxy, "https": proxy}, timeout=8)
                item["exit_ip"] = er.text.strip()[:40]
            except Exception:
                pass
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"验证未通过：{e}")
    cur, _ = load_alive()
    cur = [it for it in cur if not (it.get("ip") == ip and str(it.get("port")) == port)]
    cur.append(item)
    save_alive(cur)
    return {"ok": True, "added": item, "total": len(cur)}


@app.post("/api/proxies/alive/import")
async def proxy_alive_import(request: Request):
    """批量导入代理到存活池：text 支持每行 ip:port 或 JSON 数组/对象列表，自动去重，默认并发验证"""
    body = await request.json()
    text = body.get("text") or ""
    verify = body.get("verify", True)
    parsed = []
    if isinstance(text, list):
        for item in text:
            if isinstance(item, dict):
                parsed.append((str(item.get("ip") or "").strip(), str(item.get("port") or "").strip(),
                               (item.get("protocol") or "http").strip().lower()))
            else:
                s = str(item).strip()
                if ":" in s and s.rsplit(":", 1)[1].isdigit():
                    p1, p2 = s.rsplit(":", 1)
                    parsed.append((p1.strip(), p2.strip(), "http"))
    else:
        for line in str(text).splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line and line.rsplit(":", 1)[1].isdigit():
                p1, p2 = line.rsplit(":", 1)
                parsed.append((p1.strip(), p2.strip(), "http"))
    if not parsed:
        raise HTTPException(status_code=400, detail="未解析到有效代理（每行 ip:port）")
    cur, _ = load_alive()
    keys = {f"{it.get('ip')}:{it.get('port')}" for it in cur}
    new = [p for p in parsed if f"{p[0]}:{p[1]}" not in keys]
    if not new:
        return {"ok": True, "imported": len(parsed), "alive": 0, "duplicated": len(parsed), "total": len(cur),
                "msg": "导入的代理已全部存在于存活池"}
    items = [{"ip": ip, "port": port, "protocol": proto, "ms": None, "exit_ip": None,
              "checked_at": time.strftime("%Y-%m-%d %H:%M:%S")} for ip, port, proto in new]
    if verify:
        def test_one(it):
            proxy = f"{it['protocol']}://{it['ip']}:{it['port']}"
            try:
                r = requests.get("https://www.baidu.com", proxies={"http": proxy, "https": proxy},
                                 timeout=8, headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code != 200:
                    return None
                it["ms"] = int(r.elapsed.total_seconds() * 1000)
                try:
                    er = requests.get(ECHO_SERVICE, proxies={"http": proxy, "https": proxy}, timeout=8)
                    it["exit_ip"] = er.text.strip()[:40]
                except Exception:
                    pass
                return it
            except Exception:
                return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            verified = [it for it in ex.map(test_one, items) if it]
        if not verified:
            raise HTTPException(status_code=400, detail="导入的代理全部未通过验证，未入库（可点「仅入库不验证」）")
        items = verified
    cur = [it for it in cur if f"{it.get('ip')}:{it.get('port')}" not in {f"{i['ip']}:{i['port']}" for i in items}]
    cur += items
    save_alive(cur)
    return {"ok": True, "imported": len(parsed), "alive": len(items),
            "duplicated": len(parsed) - len(new), "dead": len(new) - len(items), "total": len(cur)}


@app.get("/api/proxies/alive/export")
def proxy_alive_export(request: Request, format: str = "csv"):
    """导出存活代理池（csv / json 下载）"""
    items, updated_at = load_alive()
    if not items:
        raise HTTPException(status_code=404, detail="存活池为空")
    if format == "json":
        content = json.dumps({"updated_at": updated_at, "total": len(items), "items": items},
                             ensure_ascii=False, indent=2)
        return Response(content, media_type="application/json",
                        headers={"Content-Disposition": 'attachment; filename="alive_proxies.json"'})
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ip", "port", "protocol", "ms", "exit_ip", "checked_at"])
    for it in items:
        w.writerow([it.get("ip", ""), it.get("port", ""), it.get("protocol", "http"),
                    it.get("ms", ""), it.get("exit_ip", ""), it.get("checked_at", "")])
    return Response(buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": 'attachment; filename="alive_proxies.csv"'})




# =============== 住宅节点（应用众包：第三方应用静默上报用户设备出口，汇聚为平台自己的住宅代理源） ===============

RESIDENTIAL_FILE = "/opt/scrapling/residential_nodes.json"
RESIDENTIAL_APPS_FILE = "/opt/scrapling/residential_apps.json"
RESIDENTIAL_STALE_HOURS = 48   # 节点超过该时长无心跳视为离线，不再参与选用

def load_residential_nodes():
    try:
        with open(RESIDENTIAL_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("nodes") or []
    except Exception:
        return []

def save_residential_nodes(nodes):
    try:
        with open(RESIDENTIAL_FILE, "w", encoding="utf-8") as f:
            json.dump({"nodes": nodes}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_residential_apps():
    try:
        with open(RESIDENTIAL_APPS_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("apps") or []
    except Exception:
        return []

def save_residential_apps(apps):
    try:
        with open(RESIDENTIAL_APPS_FILE, "w", encoding="utf-8") as f:
            json.dump({"apps": apps}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _node_fresh(n, now=None):
    """节点是否新鲜：verified 且 48h 内有心跳（last_seen 由上报/心跳刷新）"""
    if n.get("status") != "verified":
        return False
    try:
        last = time.mktime(time.strptime(n.get("last_seen") or "", "%Y-%m-%d %H:%M:%S"))
    except Exception:
        return False
    if now is None:
        now = time.time()
    return (now - last) < RESIDENTIAL_STALE_HOURS * 3600

def residential_owner_of(request):
    """上报者解析：优先 X-App-Token（第三方应用静默模式），其次登录用户/API Key（自用模式）"""
    app_token = (request.headers.get("X-App-Token") or "").strip()
    if app_token:
        app = next((a for a in load_residential_apps() if a.get("token") == app_token), None)
        if app:
            return {"kind": "app", "id": app.get("id"), "name": app.get("name", "应用")}
    user = current_user(request)
    return {"kind": "user", "id": str(user.get("id")), "name": (user.get("username") or user.get("id") or "")[:40]}

@app.post("/api/residential/report")
async def residential_report(request: Request):
    """上报一个住宅节点：第三方应用内置 X-App-Token 静默上报用户设备的 ip:port；
    平台后台验证（能出网、出口不是服务器 IP）通过后入库「平台住宅池」，全局代理访问自动优先选用。
    自用模式也可用登录态/API Key 上报（归属本人）。"""
    own = residential_owner_of(request)
    body = await request.json()
    ip = (body.get("ip") or "").strip()
    port = str(body.get("port") or "").strip()
    protocol = (body.get("protocol") or "http").strip().lower() or "http"
    name = (body.get("name") or own["name"]).strip()[:40]
    if not ip or not port.isdigit():
        raise HTTPException(status_code=400, detail="请填写有效的 ip 和 port")
    node = {"node_id": uuid.uuid4().hex[:12], "owner": own["id"], "owner_kind": own["kind"],
            "owner_name": own["name"], "name": name,
            "ip": ip, "port": port, "protocol": protocol, "verified": False, "status": "pending",
            "ms": None, "exit_ip": None, "added_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "last_seen": time.strftime("%Y-%m-%d %H:%M:%S")}
    nodes = load_residential_nodes()
    nodes.append(node)
    save_residential_nodes(nodes)

    def verify():
        proxy = f"{protocol}://{ip}:{port}"
        n = None
        for it in load_residential_nodes():
            if it.get("node_id") == node["node_id"]:
                n = it
                break
        if n is None:
            return
        try:
            r = requests.get("https://www.baidu.com", proxies={"http": proxy, "https": proxy},
                             timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                n["status"] = "dead"
                n["verified"] = False
                save_residential_nodes(load_residential_nodes())
                return
            n["ms"] = int(r.elapsed.total_seconds() * 1000)
            exit_ip = check_exit_ip(proxy, timeout=6)
            if exit_ip == "LEAK":
                n["status"] = "leak"
                n["verified"] = False
                save_residential_nodes(load_residential_nodes())
                return
            n["exit_ip"] = exit_ip or ""
            n["status"] = "verified"
            n["verified"] = True
            n["last_seen"] = time.strftime("%Y-%m-%d %H:%M:%S")
            # 平台级：住宅 IP 高质量，同步进存活池，全局代理访问自动优先
            touch_alive(ip, port, protocol, n["ms"], n["exit_ip"], source="residential")
        except Exception:
            n["status"] = "dead"
            n["verified"] = False
        save_residential_nodes(load_residential_nodes())
    threading.Thread(target=verify, daemon=True).start()
    return {"ok": True, "node": node, "msg": "已接收，正在后台验证（约几秒），通过后进入平台住宅池"}

@app.get("/api/residential/list")
async def residential_list(request: Request):
    """我的节点：应用令牌 → 该应用全部节点；登录用户 → 本人节点"""
    own = residential_owner_of(request)
    mine = [n for n in load_residential_nodes() if n.get("owner") == own["id"]]
    return {"total": len(mine), "items": mine}

@app.post("/api/residential/remove")
async def residential_remove(request: Request):
    own = residential_owner_of(request)
    body = await request.json()
    nid = str(body.get("node_id") or "").strip()
    nodes = [n for n in load_residential_nodes()
             if not (n.get("owner") == own["id"] and n.get("node_id") == nid)]
    save_residential_nodes(nodes)
    return {"ok": True, "left": len(nodes)}

@app.post("/api/residential/heartbeat")
async def residential_heartbeat(request: Request):
    """客户端保活：定期心跳刷新 last_seen，超时节点自动不参与选用"""
    own = residential_owner_of(request)
    body = await request.json()
    nid = str(body.get("node_id") or "").strip()
    nodes = load_residential_nodes()
    hit = False
    for n in nodes:
        if n.get("owner") == own["id"] and n.get("node_id") == nid:
            n["last_seen"] = time.strftime("%Y-%m-%d %H:%M:%S")
            hit = True
            break
    save_residential_nodes(nodes)
    return {"ok": True, "hit": hit}

@app.get("/api/residential/pool")
async def residential_pool(request: Request):
    """平台住宅池（已通过验证且新鲜的节点，全局代理访问自动优先选用）"""
    now = time.time()
    pool = [n for n in load_residential_nodes() if _node_fresh(n, now)]
    return {"total": len(pool), "items": pool}

# ---- 管理端：应用管理 + 平台节点池 ----

@app.get("/api/admin/apps")
def admin_residential_apps(request: Request):
    require_admin(request)
    apps = load_residential_apps()
    nodes = load_residential_nodes()
    for a in apps:
        a["node_count"] = sum(1 for n in nodes if n.get("owner_kind") == "app" and n.get("owner") == a.get("id"))
    return {"apps": apps}

@app.post("/api/admin/apps")
async def admin_residential_apps_add(request: Request):
    require_admin(request)
    body = await request.json()
    name = (body.get("name") or "未命名应用").strip()[:40]
    if not name:
        raise HTTPException(status_code=400, detail="请填写应用名称")
    app = {"id": "app-" + uuid.uuid4().hex[:10], "name": name,
           "token": uuid.uuid4().hex + uuid.uuid4().hex,
           "created_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    apps = load_residential_apps()
    apps.append(app)
    save_residential_apps(apps)
    return {"ok": True, "app": app, "msg": "已创建。把 App Token 内置进你的应用：应用启动后带 X-App-Token 头静默上报用户住宅 IP"}

@app.delete("/api/admin/apps/{aid}")
def admin_residential_apps_del(request: Request, aid: str):
    require_admin(request)
    apps = [a for a in load_residential_apps() if a.get("id") != aid]
    save_residential_apps(apps)
    # 同步下线该应用的节点（标记 dead）
    nodes = load_residential_nodes()
    for n in nodes:
        if n.get("owner_kind") == "app" and n.get("owner") == aid:
            n["status"] = "removed"
            n["verified"] = False
    save_residential_nodes(nodes)
    return {"ok": True}

@app.get("/api/admin/residential-nodes")
def admin_residential_nodes(request: Request, app: str = "", status: str = "", limit: int = 200):
    require_admin(request)
    nodes = load_residential_nodes()
    # 默认过滤已下线（removed）节点；显式传 status=removed 可查看全部
    if not status:
        nodes = [n for n in nodes if n.get("status") != "removed"]
    if app:
        nodes = [n for n in nodes if n.get("owner") == app]
    if status:
        nodes = [n for n in nodes if n.get("status") == status]
    nodes.sort(key=lambda x: x.get("last_seen") or "", reverse=True)
    return {"total": len(nodes), "items": nodes[:limit]}

@app.post("/api/admin/residential-nodes/{nid}/remove")
def admin_residential_nodes_remove(request: Request, nid: str):
    require_admin(request)
    nodes = [n for n in load_residential_nodes() if n.get("node_id") != nid]
    save_residential_nodes(nodes)
    return {"ok": True, "left": len(nodes)}


# =============== 付费代理源（管理后台配置；API 密钥可暂不填写） ===============

PAID_SOURCES_FILE = "/opt/scrapling/paid_sources.json"
_paid_pull_lock = threading.Lock()

def load_paid_sources():
    try:
        with open(PAID_SOURCES_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("sources") or []
    except Exception:
        return []

def save_paid_sources(sources):
    try:
        with open(PAID_SOURCES_FILE, "w", encoding="utf-8") as f:
            json.dump({"sources": sources}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def pull_paid_source(src):
    """从付费代理提取 API 拉一批代理入库主池。支持 JSON {data:[{ip,port}]} 或纯文本每行 ip:port"""
    api_url = (src.get("api_url") or "").strip()
    api_key = (src.get("api_key") or "").strip()
    if not api_url:
        return {"ok": False, "msg": "未配置提取接口地址"}
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        if api_key:
            headers["Authorization"] = "Bearer " + api_key
        r = requests.get(api_url, headers=headers, timeout=30)
        if r.status_code != 200:
            return {"ok": False, "msg": "HTTP " + str(r.status_code)}
        text = r.text[:2000000]
        parsed = []
        try:
            obj = json.loads(text)
            data = obj.get("data") or obj.get("proxies") or obj.get("list") or (obj if isinstance(obj, list) else [])
            for it in data:
                if isinstance(it, dict):
                    ip = str(it.get("ip") or it.get("host") or "").strip()
                    port = str(it.get("port") or "").strip()
                    if ip and port.isdigit():
                        parsed.append((ip, port, (it.get("protocol") or "http").strip().lower() or "http"))
                else:
                    s = str(it).strip()
                    if ":" in s and s.rsplit(":", 1)[1].isdigit():
                        p1, p2 = s.rsplit(":", 1)
                        parsed.append((p1.strip(), p2.strip(), "http"))
        except Exception:
            for line in text.splitlines():
                line = line.strip()
                if ":" in line and line.rsplit(":", 1)[1].isdigit():
                    p1, p2 = line.rsplit(":", 1)
                    parsed.append((p1.strip(), p2.strip(), "http"))
        if not parsed:
            return {"ok": False, "msg": "响应中未解析到代理"}
        pool = load_proxies()
        keys = {f"{p.get('ip')}:{p.get('port')}" for p in pool}
        added = 0
        for ip, port, proto in parsed:
            if f"{ip}:{port}" not in keys:
                pool.append({"ip": ip, "port": port, "protocol": proto,
                             "source": (src.get("name") or "paid")[:30],
                             "added_at": time.strftime("%Y-%m-%d %H:%M:%S")})
                added += 1
        if added:
            try:
                with open(PROXY_JSON, "w", encoding="utf-8") as f:
                    json.dump(pool, f, ensure_ascii=False)
            except Exception as e:
                return {"ok": False, "msg": "入库失败: " + str(e)[:100]}
        return {"ok": True, "total": len(parsed), "added": added}
    except Exception as e:
        return {"ok": False, "msg": str(e)[:200]}



# ---------------- 定时调度 / Webhook / 变更监控 ----------------

SCHEDULES_FILE = "/opt/scrapling/schedules.json"
WEBHOOKS_FILE = "/opt/scrapling/webhooks.json"

def load_schedules():
    try:
        with open(SCHEDULES_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("schedules") or []
    except Exception:
        return []

def save_schedules(items):
    try:
        with open(SCHEDULES_FILE, "w", encoding="utf-8") as f:
            json.dump({"schedules": items}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_webhooks():
    try:
        with open(WEBHOOKS_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("webhooks") or []
    except Exception:
        return []

def save_webhooks(items):
    try:
        with open(WEBHOOKS_FILE, "w", encoding="utf-8") as f:
            json.dump({"webhooks": items}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def cron_field_match(expr, val):
    """单字段 cron 匹配：* / */n 数字 逗号 区间"""
    expr = (expr or "").strip()
    if expr in ("", "*"):
        return True
    if expr.startswith("*/"):
        try:
            return val % int(expr[2:]) == 0
        except Exception:
            return False
    if "," in expr:
        try:
            return val in [int(x) for x in expr.split(",")]
        except Exception:
            return False
    if "-" in expr:
        try:
            a, b = [int(x) for x in expr.split("-")]
            return a <= val <= b
        except Exception:
            return False
    try:
        return int(expr) == val
    except Exception:
        return False

def cron_match(expr, ts=None):
    """5 字段 cron：分 时 日 月 周（周：0=周日至6=周六）；ts 为 epoch 秒，默认当前时间"""
    if ts is None:
        ts = time.time()
    lt = time.localtime(ts)
    wday = (lt.tm_wday + 1) % 7  # 周1=1 ... 周日=0
    parts = (expr or "").split()
    if len(parts) != 5:
        return False
    return (cron_field_match(parts[0], lt.tm_min) and cron_field_match(parts[1], lt.tm_hour)
            and cron_field_match(parts[2], lt.tm_mday) and cron_field_match(parts[3], lt.tm_mon)
            and cron_field_match(parts[4], wday))

def fire_webhooks(event, payload):
    for w in load_webhooks():
        if not w.get("enabled"):
            continue
        evs = w.get("events") or ["task.finish"]
        if event not in evs:
            continue
        try:
            requests.post(w["url"], json={"event": event, **payload},
                          headers={"X-Webhook-Secret": w.get("secret", ""),
                                   "User-Agent": "scrapling-webhook/1.0"},
                          timeout=10)
        except Exception:
            pass

def _task_items(tid):
    fp = os.path.join(task_dir(tid), "items.jsonl")
    if not os.path.exists(fp):
        return [], ""
    try:
        lines = open(fp, "r", encoding="utf-8", errors="replace").readlines()
        return lines, "".join(lines)
    except Exception:
        return [], ""

def _schedule_loop():
    """每 30 秒扫一次定时计划：命中 cron 即启动任务；任务完成后对比结果做变更监控 + 触发 Webhook"""
    while True:
        try:
            _ts = time.time()
            key = time.strftime("%Y%m%d%H%M", time.localtime(_ts))
            for s in load_schedules():
                if not s.get("enabled"):
                    continue
                if s.get("last_run") == key:
                    continue
                if not cron_match(s.get("cron", ""), _ts):
                    continue
                task = load_task(s.get("task_id") or "")
                if task is None:
                    continue
                try:
                    start_container(task)
                    s["last_run"] = key
                    s["last_run_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(_ts))
                    s["runs"] = int(s.get("runs", 0)) + 1
                    save_schedules(load_schedules())
                    fire_webhooks("task.scheduled", {"schedule_id": s.get("id"),
                                                     "task_id": s.get("task_id"),
                                                     "cron": s.get("cron")})
                except Exception:
                    pass
            # 变更监控：已完成的计划任务，对比本次/上次结果
            for s in load_schedules():
                if not s.get("monitor"):
                    continue
                tid = s.get("task_id") or ""
                task = load_task(tid)
                if task is None:
                    continue
                if docker_status(tid) != "exited":
                    continue
                lines, content = _task_items(tid)
                digest = str(hash(content))
                if s.get("_last_digest") is not None and s.get("_last_digest") != digest:
                    fire_webhooks("task.changed", {"schedule_id": s.get("id"),
                                                   "task_id": tid,
                                                   "items": len(lines),
                                                   "changed": True})
                s["_last_digest"] = digest
                s["last_items"] = len(lines)
                save_schedules(load_schedules())
            # 任务完成检测（任意任务 running→exited）：触发 task.finish Webhook
            for t in load_all_tasks_meta():
                st = docker_status(t.get("id"))
                prev = t.get("_last_status")
                if prev == "running" and st == "exited":
                    fire_webhooks("task.finish", {"task_id": t.get("id"),
                                                  "name": t.get("name"),
                                                  "type": t.get("type"),
                                                  "items": task_item_count(t.get("id"))})
            _mark_task_statuses()
        except Exception:
            pass
        time.sleep(30)

def _mark_task_statuses():
    try:
        tasks = load_all_tasks_meta()
        for t in tasks:
            t["_last_status"] = docker_status(t.get("id"))
    except Exception:
        pass

def load_all_tasks_meta():
    items = []
    if not os.path.isdir(TASKS_DIR):
        return items
    for d in os.listdir(TASKS_DIR):
        t = load_task(d)
        if t:
            items.append(t)
    return items

# 启动计划调度线程（幂等）
_schedule_thread_started = False

def _ensure_schedule_thread():
    global _schedule_thread_started
    if _schedule_thread_started:
        return
    threading.Thread(target=_schedule_loop, daemon=True).start()
    _schedule_thread_started = True

# ---- 定时计划 API ----

@app.post("/api/schedules")
async def api_schedule_create(request: Request):
    require_quota(request, cost=0)
    body = await request.json()
    task_id = (body.get("task_id") or "").strip()
    cron = (body.get("cron") or "").strip()
    name = (body.get("name") or "").strip() or "定时任务"
    monitor = bool(body.get("monitor", False))
    if not task_id or not cron:
        raise HTTPException(status_code=400, detail="请填写任务ID与cron表达式")
    if load_task(task_id) is None:
        raise HTTPException(status_code=400, detail="任务不存在")
    if len(cron.split()) != 5:
        raise HTTPException(status_code=400, detail="cron 需 5 段：分 时 日 月 周，如 */30 * * * * 或 0 9 * * *")
    sch = {"id": "s" + uuid.uuid4().hex[:8], "name": name, "task_id": task_id, "cron": cron,
           "monitor": monitor, "enabled": True, "runs": 0,
           "created_at": time.strftime("%Y-%m-%d %H:%M:%S"), "last_run": "", "last_run_at": ""}
    items = load_schedules()
    items.append(sch)
    save_schedules(items)
    _ensure_schedule_thread()
    return {"ok": True, "schedule": sch, "msg": "已创建。示例 cron：*/30 * * * *（每30分钟）、0 9 * * *（每天9点）"}

@app.get("/api/schedules")
def api_schedules():
    return {"schedules": load_schedules()}

@app.delete("/api/schedules/{sid}")
def api_schedule_del(sid: str):
    items = [s for s in load_schedules() if s.get("id") != sid]
    save_schedules(items)
    return {"ok": True}

@app.post("/api/schedules/{sid}/toggle")
def api_schedule_toggle(sid: str):
    items = load_schedules()
    for s in items:
        if s.get("id") == sid:
            s["enabled"] = not s.get("enabled", True)
            break
    save_schedules(items)
    return {"ok": True}

# ---- Webhook API ----

@app.post("/api/webhooks")
async def api_webhook_create(request: Request):
    require_quota(request, cost=0)
    body = await request.json()
    url = (body.get("url") or "").strip()
    if not url.startswith("http"):
        raise HTTPException(status_code=400, detail="请填写 http(s) 回调地址")
    wh = {"id": "w" + uuid.uuid4().hex[:8], "url": url,
          "secret": (body.get("secret") or "").strip(),
          "events": body.get("events") or ["task.finish"],
          "enabled": True, "created_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    items = load_webhooks()
    items.append(wh)
    save_webhooks(items)
    return {"ok": True, "webhook": wh}

@app.get("/api/webhooks")
def api_webhooks():
    return {"webhooks": load_webhooks()}

@app.delete("/api/webhooks/{wid}")
def api_webhook_del(wid: str):
    items = [w for w in load_webhooks() if w.get("id") != wid]
    save_webhooks(items)
    return {"ok": True}

@app.post("/api/webhooks/{wid}/toggle")
def api_webhook_toggle(wid: str):
    items = load_webhooks()
    for w in items:
        if w.get("id") == wid:
            w["enabled"] = not w.get("enabled", True)
            break
    save_webhooks(items)
    return {"ok": True}

@app.post("/api/webhooks/test")
async def api_webhook_test(request: Request):
    body = await request.json()
    url = (body.get("url") or "").strip()
    if not url.startswith("http"):
        raise HTTPException(status_code=400, detail="请填写 http(s) 回调地址")
    try:
        r = requests.post(url, json={"event": "test", "msg": "Scrapling Webhook 测试"},
                          headers={"User-Agent": "scrapling-webhook/1.0"}, timeout=10)
        return {"ok": True, "status": r.status_code}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"回调失败: {e}")

_ensure_schedule_thread()



# ---------------- 批次C：Excel导出 / DOM导航 / 数据清洗 / IP地区打标 ----------------

GEOIP_CACHE_FILE = "/opt/scrapling/geoip_cache.json"
REGION_LOCK = threading.Lock()

def load_geo_cache():
    try:
        with open(GEOIP_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_geo_cache(c):
    try:
        with open(GEOIP_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(c, f, ensure_ascii=False)
    except Exception:
        pass

def _public_ip_host(ip):
    """去掉端口取纯 IP"""
    ip = (ip or "").strip()
    if ":" in ip and ip.count(":") == 1:
        ip = ip.split(":")[0]
    return ip

def lookup_ip_region(ip, cache):
    host = _public_ip_host(ip)
    if not host or host in cache:
        return cache.get(host, {})
    try:
        r = requests.get("http://ip-api.com/json/" + host + "?fields=status,country,countryCode,regionName,city,query", timeout=8)
        d = r.json()
        if d.get("status") == "success":
            info = {"country": d.get("country"), "code": d.get("countryCode"),
                    "region": d.get("regionName"), "city": d.get("city")}
            cache[host] = info
            return info
    except Exception:
        pass
    return {}

def _region_loop():
    """后台增量给代理打地区标签：优先存活池，再全池（ip-api 免费 45次/分钟，限速）"""
    while True:
        try:
            alive, _ = load_alive()
            allp = load_json_quiet(PROXY_JSON) or []
            if isinstance(allp, dict):
                allp = allp.get("proxies") or allp.get("items") or []
            cache = load_geo_cache()
            changed = False
            targets = [p for p in alive if p and not _has_region(p, cache)] + [p for p in allp if p and not _has_region(p, cache)]
            targets = targets[:120]  # 每轮最多打 120 条（约2-3分钟）
            for p in targets:
                ip = p.get("ip") or (p.get("server") or "").split(":")[0]
                if not ip:
                    continue
                info = lookup_ip_region(ip, cache)
                if info:
                    p["country"] = info.get("country")
                    p["country_code"] = info.get("code")
                    p["region"] = info.get("region")
                    p["city"] = info.get("city")
                    changed = True
                time.sleep(1.4)  # 限速 ~40/min
            if changed:
                save_alive(alive)
                save_json_quiet(PROXY_JSON, allp)
                save_geo_cache(cache)
        except Exception:
            pass
        time.sleep(240)

def _has_region(p, cache):
    if p.get("country"):
        return True
    ip = p.get("ip") or (p.get("server") or "").split(":")[0]
    return _public_ip_host(ip) in cache

_region_thread_started = False

def _ensure_region_thread():
    global _region_thread_started
    if _region_thread_started:
        return
    threading.Thread(target=_region_loop, daemon=True).start()
    _region_thread_started = True

def load_json_quiet(fp):
    try:
        with open(fp, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_json_quiet(fp, data):
    try:
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ---- Excel 导出 ----

def items_to_xlsx_bytes(items, filename="scrapling_export"):
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "抓取结果"
    keys = []
    for it in items:
        if isinstance(it, dict):
            for k in it.keys():
                if k not in keys:
                    keys.append(k)
    if not keys:
        keys = ["url", "title", "content"]
    ws.append(keys)
    for it in items:
        row = []
        for k in keys:
            v = it.get(k) if isinstance(it, dict) else it
            row.append(v if v is not None else "")
        ws.append(row)
    import io as _io
    buf = _io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# ---- DOM 导航 API ----

@app.post("/api/dom/navigate")
async def api_dom_navigate(request: Request):
    require_quota(request, cost=1)
    body = await request.json()
    url = (body.get("url") or "").strip()
    selector = (body.get("selector") or "").strip() or "html"
    action = (body.get("action") or "").strip() or "info"   # info/parent/siblings/children/attr/text/tree
    attr = (body.get("attr") or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="请填写网址")
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"})
        html = r.text
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"获取页面失败: {e}")
    from lxml import html as lhtml
    from lxml import etree
    try:
        doc = lhtml.fromstring(html.encode("utf-8", errors="ignore"))
        nodes = doc.cssselect(selector) if selector else []
        if not nodes:
            # 尝试 XPath
            try:
                nodes = doc.xpath(selector)
            except Exception:
                nodes = []
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"选择器无效: {e}")
    if not nodes:
        return {"ok": True, "count": 0, "note": "未匹配到元素", "samples": []}
    result = []
    for n in nodes[:20]:
        tag = getattr(n, "tag", "")
        text = (n.text_content() or "").strip()[:200] if action in ("text", "info", "children", "siblings") else ""
        if action == "info":
            result.append({"tag": tag, "id": n.get("id"), "class": n.get("class"),
                           "href": n.get("href"), "text": text})
        elif action == "attr":
            result.append({"attr": attr, "value": n.get(attr)})
        elif action == "text":
            result.append({"text": text})
        elif action == "parent":
            p = n.getparent()
            result.append({"parent_tag": getattr(p, "tag", None) if p is not None else None,
                           "parent_text": (p.text_content() or "").strip()[:200] if p is not None else ""})
        elif action == "children":
            kids = [{"tag": getattr(c, "tag", ""), "text": (c.text_content() or "").strip()[:80]} for c in n.getchildren()[:30]]
            result.append({"children": kids, "count": len(list(n.iterchildren()))})
        elif action == "siblings":
            sibs = [{"tag": getattr(c, "tag", ""), "text": (c.text_content() or "").strip()[:80]}
                    for c in (n.getparent().getchildren() if n.getparent() is not None else []) if c is not n][:30]
            result.append({"siblings": sibs})
        elif action == "tree":
            def mini(el, depth=0):
                return {"tag": getattr(el, "tag", ""), "depth": depth,
                        "id": el.get("id"), "cls": el.get("class"),
                        "text": (el.text_content() or "").strip()[:60]}
            result.append({"tree": [mini(c, 1) for c in n.iterchildren()][:50]})
    return {"ok": True, "count": len(nodes), "samples": result, "suggest_css": _suggest_css(nodes[0])}

def _suggest_css(node):
    parts = []
    try:
        n = node
        for _ in range(4):
            if n is None or getattr(n, "getparent", None) is None:
                break
            tag = getattr(n, "tag", "")
            if not isinstance(tag, str):
                break
            sel = tag
            if n.get("id"):
                sel = "#" + n.get("id")
                parts.insert(0, sel)
                break
            elif n.get("class"):
                sel += "." + ".".join(n.get("class").split()[:2])
            parts.insert(0, sel)
            n = n.getparent()
    except Exception:
        pass
    return " > ".join(parts) if parts else ""

# ---- 数据清洗 / 去重管道 ----

@app.post("/api/tasks/{tid}/clean")
async def api_task_clean(tid: str, request: Request):
    require_quota(request, cost=0)
    body = {}
    try:
        body = await request.json()
    except Exception:
        body = {}
    dedup_keys = body.get("dedup_keys") or []
    rules = body.get("rules") or []      # [{"field": "...", "trim": true, "drop_empty": true, "regex": "...", "replace": "..."}]
    items, _ = _task_items(tid)
    parsed = []
    for line in items:
        try:
            parsed.append(json.loads(line))
        except Exception:
            pass
    seen = {}
    out = []
    for it in parsed:
        if not isinstance(it, dict):
            out.append(it)
            continue
        keep = True
        for r in rules:
            f = r.get("field")
            if f not in it:
                continue
            v = it.get(f)
            if r.get("trim") and isinstance(v, str):
                v = v.strip()
            if r.get("drop_empty") and (v is None or (isinstance(v, str) and not v)):
                keep = False
                break
            if r.get("regex"):
                import re
                m = re.search(r["regex"], str(v or ""))
                v = m.group(0) if m else v
            if r.get("replace"):
                v = str(v or "").replace(r["replace"][0], r["replace"][1])
            it[f] = v
        if dedup_keys:
            key = tuple(str(it.get(k, "")) for k in dedup_keys)
            if key in seen:
                continue
            seen[key] = True
        out.append(it)
    fp = os.path.join(task_dir(tid), "cleaned.jsonl")
    with open(fp, "w", encoding="utf-8") as f:
        for it in out:
            f.write(json.dumps(it, ensure_ascii=False) + chr(10))
    return {"ok": True, "input": len(parsed), "output": len(out),
            "dropped": len(parsed) - len(out), "file": fp}

# ---- IP 地区选择（代理访问按地区） ----

@app.get("/api/proxies/regions")
def api_proxy_regions():
    alive, _ = load_alive()
    regions = {}
    for p in alive:
        c = p.get("country") or "未知"
        regions.setdefault(c, []).append(p)
    _ensure_region_thread()
    return {"total": len(alive), "regions": {k: len(v) for k, v in regions.items()}}

@app.get("/api/proxies/by-region")
def api_proxy_by_region(country: str = ""):
    alive, _ = load_alive()
    if country:
        alive = [p for p in alive if (p.get("country") or "") == country or (p.get("country_code") or "") == country]
    # 只返回元信息，不泄露完整代理串给前端展示
    out = []
    for p in alive[:200]:
        out.append({"ip": p.get("ip") or (p.get("server") or "").split(":")[0],
                    "country": p.get("country"), "region": p.get("region"),
                    "city": p.get("city"), "latency": p.get("latency"),
                    "full": p.get("server") or p.get("ip")})
    return {"count": len(alive), "proxies": out}

def _paid_source_loop():
    """后台守护线程：按间隔拉取已启用且已填提取地址的付费源（未配置密钥时仅提示，不影响运行）"""
    while True:
        try:
            for src in load_paid_sources():
                if not src.get("enabled", True):
                    continue
                if not (src.get("api_url") or "").strip():
                    continue
                interval = max(1, int(src.get("interval_hours", 6) or 6)) * 3600
                last = src.get("last_pull")
                now = time.time()
                if last and (now - float(last)) < interval:
                    continue
                with _paid_pull_lock:
                    res = pull_paid_source(src)
                    for s in load_paid_sources():
                        if s.get("id") == src.get("id"):
                            s["last_pull"] = time.time()
                            s["last_result"] = res.get("msg") or ("OK" if res.get("ok") else "失败")
                            break
                    save_paid_sources(load_paid_sources())
        except Exception as e:
            print("paid source loop:", e)
        time.sleep(3600)

@app.get("/api/admin/proxy-sources")
def admin_proxy_sources(request: Request):
    require_admin(request)
    srcs = []
    for s in load_paid_sources():
        s2 = dict(s)
        if s2.get("api_key"):
            s2["api_key"] = "****"
        srcs.append(s2)
    return {"sources": srcs}

@app.post("/api/admin/proxy-sources")
async def admin_proxy_sources_add(request: Request):
    require_admin(request)
    body = await request.json()
    name = (body.get("name") or "付费源").strip()[:40]
    api_url = (body.get("api_url") or "").strip()
    if not api_url:
        raise HTTPException(status_code=400, detail="请填写提取接口地址")
    src = {"id": uuid.uuid4().hex[:10], "name": name, "api_url": api_url,
           "api_key": (body.get("api_key") or "").strip(),
           "interval_hours": max(1, min(int(body.get("interval_hours", 6) or 6), 72)),
           "enabled": body.get("enabled", True),
           "added_at": time.strftime("%Y-%m-%d %H:%M:%S"), "last_pull": None, "last_result": ""}
    srcs = load_paid_sources()
    srcs.append(src)
    save_paid_sources(srcs)
    return {"ok": True, "source": src, "msg": "已保存。密钥可暂不填写；填写并启用后系统按间隔自动拉取入库"}

@app.delete("/api/admin/proxy-sources/{sid}")
def admin_proxy_sources_del(request: Request, sid: str):
    require_admin(request)
    srcs = [s for s in load_paid_sources() if s.get("id") != sid]
    save_paid_sources(srcs)
    return {"ok": True}

@app.post("/api/admin/proxy-sources/{sid}/pull")
def admin_proxy_sources_pull(request: Request, sid: str):
    require_admin(request)
    src = next((s for s in load_paid_sources() if s.get("id") == sid), None)
    if not src:
        raise HTTPException(status_code=404, detail="未找到该源")
    res = pull_paid_source(src)
    for s in load_paid_sources():
        if s.get("id") == sid:
            s["last_pull"] = time.time()
            s["last_result"] = res.get("msg") or ("OK" if res.get("ok") else "失败")
            break
    save_paid_sources(load_paid_sources())
    return {"ok": res.get("ok", False), **res}



@app.post("/api/proxies/refresh")
def proxy_refresh(request: Request):
    """后台重新爬取代理"""
    require_quota(request)
    def run():
        try:
            subprocess.run(
                ["sudo", "docker", "run", "--rm", "--entrypoint", "/app/.venv/bin/python",
                 "-v", "/opt/scrapling:/work", "pyd4vinci/scrapling", "/work/proxy_scraper.py"],
                capture_output=True, text=True, timeout=600)
        except Exception as e:
            print("refresh failed:", e)
    threading.Thread(target=run, daemon=True).start()
    return {"ok": True, "msg": "已在后台启动爬取，稍后刷新页面查看"}

# ---------------- 代理访问（代理池 IP 打开网址） ----------------

PROXY_VISIT_LOG = "/opt/scrapling/proxy_visit_log.jsonl"
TARGET_USABLE_FILE = "/opt/scrapling/target_usable.json"
ALIVE_FILE = "/opt/scrapling/alive_proxies.json"        # 存活代理池（存活检测通过的自动入库）
ECHO_SERVICE = "http://members.3322.org/dyndns/getip"   # 出口 IP 回显（纯文本，国内可达）


def load_alive():
    """读取存活代理池"""
    try:
        with open(ALIVE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("items") or [], data.get("updated_at") or ""
    except Exception:
        return [], ""


def save_alive(items, updated_at=None):
    """写入存活代理池（按延迟排序，去重保留最新）"""
    if updated_at is None:
        updated_at = time.strftime("%Y-%m-%d %H:%M:%S")
    seen = {}
    for it in items:
        key = f"{it.get('ip')}:{it.get('port')}"
        seen[key] = it
    merged = list(seen.values())
    merged.sort(key=lambda x: (x.get("ms") or 999999))
    try:
        with open(ALIVE_FILE, "w", encoding="utf-8") as f:
            json.dump({"updated_at": updated_at, "items": merged[:500]}, f, ensure_ascii=False)
    except Exception:
        pass



def mark_proxy_dead(proxy_url):
    """访问失败的代理：从存活池移除，避免反复选中"""
    try:
        pp = (proxy_url or "").split("://")[-1]
        if ":" not in pp:
            return
        ip, port = pp.rsplit(":", 1)
        alive, _ = load_alive()
        new = [a for a in alive if not (a.get("ip") == ip and str(a.get("port")) == str(port))]
        if len(new) != len(alive):
            save_alive(new)
    except Exception:
        pass

def touch_alive(ip, port, protocol="http", ms=None, exit_ip=None, source=""):
    """把「真实访问成功」的代理写进存活池（越用越准）"""
    if not ip or not port:
        return
    cur, _ = load_alive()
    for it in cur:
        if it.get("ip") == ip and str(it.get("port")) == str(port):
            if ms is not None:
                it["ms"] = int(ms)
            if exit_ip:
                it["exit_ip"] = exit_ip
            it["checked_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            save_alive(cur)
            return
    cur.append({"ip": ip, "port": str(port), "protocol": protocol, "source": source,
                "ms": int(ms) if ms is not None else 0,
                "exit_ip": exit_ip or "", "checked_at": time.strftime("%Y-%m-%d %H:%M:%S")})
    save_alive(cur)


def pick_proxy_candidates(url, n=5):
    """按质量排序返回候选代理列表：住宅池 > 该站可用池 > 存活池(延迟升序) > 全池"""
    out = []
    seen = set()
    def add(p_url):
        if p_url and p_url not in seen:
            seen.add(p_url)
            out.append(p_url)
    try:
        pool = [x for x in load_residential_nodes() if _node_fresh(x)]
        for p in pool[:n]:
            add(f"{p.get('protocol', 'http')}://{p['ip']}:{p['port']}")
    except Exception:
        pass
    try:
        if os.path.exists(TARGET_USABLE_FILE):
            data = json.load(open(TARGET_USABLE_FILE, "r", encoding="utf-8"))
            rec = data.get(url) or {}
            for p in (rec.get("ok") or [])[:n]:
                add(f"{p.get('protocol', 'http')}://{p['ip']}:{p['port']}")
    except Exception:
        pass
    try:
        alive, _ = load_alive()
        for p in sorted(alive, key=lambda x: x.get("ms") or 99999)[:n]:
            add(f"{p.get('protocol', 'http')}://{p['ip']}:{p['port']}")
    except Exception:
        pass
    pool = load_proxies()
    good = []
    for p in pool:
        ip = p.get("ip", ""); port = p.get("port", "")
        if ip and ip != "0.0.0.0" and port:
            good.append(f"{p.get('protocol', 'http')}://{ip}:{port}")
    import random as _r
    _r.shuffle(good)
    for p in good[:n]:
        add(p)
    return out

def absolutize_html(html, base_url):
    """把 HTML 中相对资源地址补全为绝对 URL（供前端 srcdoc 渲染页面）"""
    try:
        from urllib.parse import urljoin
        import re as _re
        def _fix(m):
            attr, val = m.group(1), m.group(2).strip(chr(34) + chr(39))
            if val and not val.startswith(("javascript:", "#", "data:", "http://", "https://", "//", "mailto:", "tel:")):
                return f'{attr}="{urljoin(base_url, val)}"'
            return m.group(0)
        q = chr(34) + chr(39)
        return _re.sub(r"(src|href|action|data-src|poster)=" + q + r"([^" + q + r"]*)" + q, _fix, html, flags=_re.I)
    except Exception:
        return html

def pick_random_proxy():
    """从代理池随机挑一个可用代理，返回 'protocol://ip:port'"""
    pool = load_proxies()
    good = []
    for p in pool:
        ip = p.get("ip", ""); port = p.get("port", "")
        if ip and ip != "0.0.0.0" and port:
            good.append(f"{p.get('protocol', 'http')}://{ip}:{port}")
    if not good:
        return None
    random.shuffle(good)
    return good[0]

def pick_proxy_for(url, user_id=None):
    """选代理优先级：我的住宅节点 > 指定代理 > 该站可用池 > 存活代理池 > 全池随机"""
    # 0) 平台住宅池（第三方应用众包，质量最高，全局最优先）
    try:
        pool = [n for n in load_residential_nodes() if _node_fresh(n)]
        if pool:
            p = random.choice(pool)
            return f"{p.get('protocol', 'http')}://{p['ip']}:{p['port']}"
    except Exception:
        pass
    # 1) 该站已检测的可用池
    try:
        if os.path.exists(TARGET_USABLE_FILE):
            with open(TARGET_USABLE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            rec = data.get(url) or {}
            ok_list = rec.get("ok") or []
            if ok_list:
                p = random.choice(ok_list)
                return f"{p.get('protocol', 'http')}://{p['ip']}:{p['port']}"
    except Exception:
        pass
    # 2) 存活代理池
    try:
        alive, _ = load_alive()
        if alive:
            p = random.choice(alive)
            return f"{p.get('protocol', 'http')}://{p['ip']}:{p['port']}"
    except Exception:
        pass
    # 3) 全池随机
    return pick_random_proxy()

_server_ip = {"v": None}

def get_server_ip():
    """服务器本机出口 IP（无代理直连回显服务）"""
    if _server_ip["v"]:
        return _server_ip["v"]
    try:
        r = requests.get(ECHO_SERVICE, timeout=8,
                         headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            ip = r.text.strip().splitlines()[0].strip()
            if ip:
                _server_ip["v"] = ip
                return ip
    except Exception:
        pass
    return None

def check_exit_ip(proxy_url, timeout=6):
    """用代理访问回显服务，拿到出口 IP
    返回：正常出口 IP / "LEAK"（出口=服务器IP，透明代理，必须判死）/ None（回显服务不可达，出口未知，不判死）
    """
    try:
        r = requests.get(ECHO_SERVICE, proxies={"http": proxy_url, "https": proxy_url},
                         timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            ip = r.text.strip().splitlines()[0].strip()
            if not ip:
                return None
            srv = get_server_ip()
            if srv and ip == srv:
                return "LEAK"  # 透明代理泄漏
            return ip
    except Exception:
        pass
    return None

def extract_ip_from_text(text):
    """从文本里提取第一个 IPv4 地址"""
    m = re.search(r"\d{1,3}(\.\d{1,3}){3}", text or "")
    return m.group(0) if m else None

def mcp_text(result):
    """从 MCP 工具结果里提取纯文本（用于出口 IP 验证）"""
    try:
        if not result:
            return ""
        content = result.get("content") or []
        parts = []
        for c in content:
            if c.get("type") == "text" and c.get("text"):
                parts.append(c["text"])
            elif c.get("type") == "resource":
                parts.append(c.get("text") or "")
        return "\n".join(parts)
    except Exception:
        return ""

def extract_session_id(sess):
    """从 MCP open_session 结果里提取 session_id（支持顶层字段 / content[].text 内嵌 JSON / 文本正则）"""
    if isinstance(sess, dict):
        sid = sess.get("session_id") or (sess.get("output") or {}).get("session_id")
        if not sid:
            sid = sess.get("session_id") or sess.get("id")
        if not sid:
            content = sess.get("content") or []
            for c in content:
                t = c.get("text") or ""
                try:
                    obj = json.loads(t)
                    if isinstance(obj, dict):
                        sid = obj.get("session_id") or obj.get("id")
                        if sid:
                            break
                except Exception:
                    pass
                m = re.search(r"session_id\s*[:=]\s*([A-Za-z0-9_-]{6,})", t)
                if m:
                    sid = m.group(1)
                    break
        return str(sid) if sid else str(sess)
    return str(sess)

def log_proxy_visit(rec):
    try:
        with open(PROXY_VISIT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass

@app.post("/api/proxy/visit")
async def api_proxy_visit(request: Request):
    """用代理池的 IP 打开一个网址（注册页/目标页）
    mode: plain(纯HTTP) | browser(隐身浏览器渲染) | screenshot(浏览器截图)
    proxy: 可选指定代理（ip:port 或 http://ip:port），不填则自动按质量排序取候选
    retries: 失败自动换代理重试次数（默认 2，最多 5）
    """
    require_quota(request)
    body = await request.json()
    url = (body.get("url") or "").strip()
    mode = body.get("mode", "plain")
    specified = (body.get("proxy") or "").strip()
    timeout = min(max(int(body.get("timeout", 30) or 30), 5), 120)
    retries = max(1, min(int(body.get("retries", 2) or 2), 5))
    if not url:
        raise HTTPException(status_code=400, detail="请填写网址")
    if mode not in ("plain", "browser", "screenshot"):
        raise HTTPException(status_code=400, detail=f"未知模式: {mode}")

    # 候选代理：指定代理放最前，其余按质量排序（住宅 > 该站可用 > 存活低延迟 > 全池）
    candidates = []
    if specified:
        candidates.append(specified if "://" in specified else f"http://{specified}")
    candidates += pick_proxy_candidates(url, n=retries + 2)
    if not candidates:
        raise HTTPException(status_code=502, detail="代理池为空，请先在代理池页爬取代理")

    def _classify(e):
        s = str(e)
        if "Timeout" in s or "timed out" in s or "timeout" in s:
            return "代理响应超时（可能已失效）"
        if "Connection" in s or "connect" in s or "refused" in s:
            return "连接被目标/代理拒绝（代理可能已失效）"
        if "tunnel" in s or "407" in s or "403" in s:
            return "目标站拦截或代理需认证"
        if "browser" in s.lower() or "session" in s.lower():
            return "浏览器渲染异常"
        return "访问失败"

    last_err = "无可用代理"
    tried = []
    used_mode = mode
    for attempt in range(retries):
        if not candidates:
            break
        proxy_url = candidates.pop(0)
        tried.append(proxy_url)
        t0 = time.time()
        exit_ip = check_exit_ip(proxy_url, timeout=6)
        if exit_ip == "LEAK":
            last_err = f"代理 {proxy_url} 是透明代理（出口=服务器IP，会暴露真实IP），已换下一个"
            continue
        try:
            if used_mode == "plain":
                r = requests.get(url, proxies={"http": proxy_url, "https": proxy_url},
                                 timeout=min(timeout, 15),
                                 headers={"User-Agent": "Mozilla/5.0"},
                                 allow_redirects=True)
                text = r.text[:60000]
                result = {"type": "text",
                          "text": f"[HTTP {r.status_code}] 页面大小 {len(r.content)} 字节\n\n{text}",
                          "page_html": absolutize_html(text, url)}
            else:
                sess = call_tool("open_session", {
                    "session_type": "dynamic", "headless": True, "proxy": proxy_url})
                session_id = extract_session_id(sess)
                try:
                    probe = call_tool("session_fetch", {
                        "url": ECHO_SERVICE, "session_id": session_id,
                        "main_content_only": False, "timeout": 20000})
                    probe_ip = extract_ip_from_text(mcp_text(probe))
                    srv = get_server_ip()
                    if probe_ip and srv and probe_ip == srv:
                        raise Exception("browser: 代理未生效（出口=服务器IP，会暴露）")
                    if probe_ip and not exit_ip:
                        exit_ip = probe_ip
                    if used_mode == "browser":
                        result = parse_result_content(call_tool("session_fetch", {
                            "url": url, "session_id": session_id, "network_idle": True,
                            "main_content_only": False, "timeout": 25000}))
                    else:
                        result = parse_result_content(call_tool("screenshot", {
                            "url": url, "session_id": session_id, "image_type": "png",
                            "full_page": bool(body.get("full_page", False)), "network_idle": True, "timeout": 25000}))
                finally:
                    try:
                        call_tool("close_session", {"session_id": session_id})
                    except Exception:
                        pass
            ms = int((time.time() - t0) * 1000)
            log_proxy_visit({"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "url": url, "mode": used_mode,
                             "proxy": proxy_url, "exit_ip": exit_ip, "ms": ms, "ok": True})
            try:
                pp = proxy_url.split("://")[-1]
                if ":" in pp:
                    ip, port = pp.rsplit(":", 1)
                    touch_alive(ip, port, proxy_url.split("://")[0], ms, exit_ip or "")
            except Exception:
                pass
            return {"ok": True, "mode": used_mode, "proxy": proxy_url, "exit_ip": exit_ip,
                    "ms": ms, "attempts": attempt + 1, "result": result}
        except HTTPException:
            raise
        except Exception as e:
            last_err = _classify(e)
            # 记录失败代理，避免下次又选中它
            try:
                mark_proxy_dead(proxy_url)
            except Exception:
                pass

    # 浏览器/截图全失败 → 自动降级 plain 再试一次（至少拿回 HTML）
    if used_mode != "plain" and candidates:
        used_mode = "plain"
        try:
            proxy_url = candidates.pop(0)
            t0 = time.time()
            exit_ip = check_exit_ip(proxy_url, timeout=6)
            r = requests.get(url, proxies={"http": proxy_url, "https": proxy_url},
                             timeout=15, headers={"User-Agent": "Mozilla/5.0"},
                             allow_redirects=True)
            ms = int((time.time() - t0) * 1000)
            log_proxy_visit({"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "url": url, "mode": "plain",
                             "proxy": proxy_url, "exit_ip": exit_ip, "ms": ms, "ok": True})
            return {"ok": True, "mode": "plain", "proxy": proxy_url, "exit_ip": exit_ip,
                    "ms": ms, "attempts": len(tried) + 1, "result": {"type": "text",
                    "text": f"[HTTP {r.status_code}] 浏览器渲染失败已自动降级为普通请求，页面大小 {len(r.content)} 字节\n\n{r.text[:60000]}",
                    "page_html": absolutize_html(r.text[:60000], url)}}
        except Exception as e:
            last_err = _classify(e)

    log_proxy_visit({"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "url": url, "mode": used_mode,
                     "proxy": "", "exit_ip": None,
                     "ms": 0, "ok": False, "error": last_err})
    raise HTTPException(status_code=502,
                        detail=f"代理访问失败（已尝试 {len(tried)} 个代理: {last_err}）。建议：先在代理池页「开始体检」筛可用代理，或用「网页截图/普通请求」模式")

@app.post("/api/proxy/visit/records/clear")
def api_proxy_records_clear():
    try:
        os.remove(PROXY_VISIT_LOG)
    except Exception:
        pass
    return {"ok": True}

@app.get("/api/proxy/records")
def api_proxy_records(tail: int = 100):
    records = []
    if os.path.exists(PROXY_VISIT_LOG):
        with open(PROXY_VISIT_LOG, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except Exception:
                    continue
    return {"records": records[-tail:]}

# ---------------- 目标站代理体检（该网站可用代理池） ----------------

_target_check = {"running": False, "url": "", "progress": {}, "result": None}

@app.post("/api/proxy/check-target")
async def api_check_target(request: Request):
    """让整个代理池挨个访问目标网址，筛出能打开该站的代理子集"""
    require_quota(request)
    body = await request.json()
    url = (body.get("url") or "").strip()
    limit = min(max(int(body.get("limit", 100) or 100), 1), 300)
    if not url:
        raise HTTPException(status_code=400, detail="请填写目标网址")
    if _target_check["running"]:
        return {"running": True, "url": _target_check["url"]}
    pool = load_proxies()
    good = [p for p in pool if p.get("ip") and p["ip"] != "0.0.0.0" and p.get("port")]
    random.shuffle(good)
    proxies = good[:limit]
    if not proxies:
        return {"running": False, "result": {"checked": 0, "ok": 0, "reachable": 0, "items": []}}

    _target_check["running"] = True
    _target_check["url"] = url
    _target_check["progress"] = {"total": len(proxies), "done": 0, "ok": 0, "reachable": 0}
    _target_check["result"] = None

    def one(pr):
        ip, port, proto = pr["ip"], pr["port"], pr.get("protocol", "http")
        proxy = f"{proto}://{ip}:{port}"
        try:
            r = requests.get(url, proxies={"http": proxy, "https": proxy}, timeout=8,
                             headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
            code = r.status_code
            return {"ip": ip, "port": port, "protocol": proto, "source": pr.get("source", ""),
                    "code": code, "ok": code < 400, "reachable": True,
                    "ms": int(r.elapsed.total_seconds() * 1000)}
        except Exception:
            return {"ip": ip, "port": port, "protocol": proto, "source": pr.get("source", ""),
                    "code": 0, "ok": False, "reachable": False, "ms": 0}

    def run():
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as ex:
            for res in ex.map(one, proxies):
                results.append(res)
                _target_check["progress"]["done"] += 1
                if res["ok"]:
                    _target_check["progress"]["ok"] += 1
                if res["reachable"]:
                    _target_check["progress"]["reachable"] += 1
        results.sort(key=lambda x: (not x["ok"], x["ms"]))
        ok_list = [r for r in results if r["ok"]]
        try:
            data = {}
            if os.path.exists(TARGET_USABLE_FILE):
                with open(TARGET_USABLE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
            data[url] = {
                "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "ok": ok_list,
                "reachable_count": sum(1 for r in results if r["reachable"]),
                "total": len(results),
            }
            with open(TARGET_USABLE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
        _target_check["result"] = {
            "checked": len(results), "ok": len(ok_list),
            "reachable": sum(1 for r in results if r["reachable"]),
            "items": results[:300],
        }
        _target_check["running"] = False

    threading.Thread(target=run, daemon=True).start()
    return {"running": True, "total": len(proxies), "url": url}

@app.get("/api/proxy/check-target/status")
def api_check_target_status():
    return {"running": _target_check["running"], "url": _target_check["url"],
            "progress": _target_check["progress"], "result": _target_check["result"]}

@app.get("/api/proxy/target-usable")
def api_target_usable(url: str = ""):
    """查询某目标站已检测出的可用代理"""
    if not os.path.exists(TARGET_USABLE_FILE):
        return {"items": []}
    with open(TARGET_USABLE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    if url and url in data:
        return {"url": url, "items": data[url].get("ok", []),
                "checked_at": data[url].get("checked_at"), "total": data[url].get("total", 0)}
    if data:
        k = list(data.keys())[-1]
        return {"url": k, "items": data[k].get("ok", []),
                "checked_at": data[k].get("checked_at"), "total": data[k].get("total", 0)}
    return {"items": []}

# ---------------- Spider 任务管理 ----------------

TASKS_DIR = "/opt/scrapling/tasks"
TASK_TYPES = {
    "crawl": "全站爬取（跟随链接）",
    "single": "单页/指定页提取",
    "sitemap": "站点地图（Sitemap）",
    "xml": "XML/RSS Feed",
    "csv": "CSV Feed",
    "shopify": "Shopify 商店产品",
}
DOCKER_BASE = ["sudo", "docker", "run", "-d", "--restart", "no",
               "--entrypoint", "/bin/sh",
               "-v", "/opt/scrapling:/work"]


def task_dir(tid):
    return os.path.join(TASKS_DIR, tid)


def load_task(tid):
    fp = os.path.join(task_dir(tid), "task.json")
    if not os.path.exists(fp):
        return None
    try:
        with open(fp, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_task(task):
    os.makedirs(task_dir(task["id"]), exist_ok=True)
    with open(os.path.join(task_dir(task["id"]), "task.json"), "w", encoding="utf-8") as f:
        json.dump(task, f, ensure_ascii=False, indent=2)


def container_name(tid):
    return f"scrapling-task-{tid}"


def docker_status(tid):
    name = container_name(tid)
    try:
        out = subprocess.run(["sudo", "docker", "inspect", "-f", "{{.State.Status}}", name],
                             capture_output=True, text=True, timeout=15)
        st = out.stdout.strip()
        if st == "running":
            return "running"
        if st == "exited":
            # 区分暂停(137/SIGINT)与完成(0)
            code = subprocess.run(["sudo", "docker", "inspect", "-f", "{{.State.ExitCode}}", name],
                                  capture_output=True, text=True, timeout=15).stdout.strip()
            return "paused" if code == "137" else "exited"
        return st or "missing"
    except Exception:
        return "missing"


def task_item_count(tid):
    fp = os.path.join(task_dir(tid), "items.jsonl")
    if not os.path.exists(fp):
        return 0
    try:
        return sum(1 for _ in open(fp, "r", encoding="utf-8", errors="replace"))
    except Exception:
        return 0


def enrich_task(task):
    t = dict(task)
    t["status"] = docker_status(t["id"])
    t["item_count"] = task_item_count(t["id"])
    t["log_size"] = 0
    logp = os.path.join(task_dir(t["id"]), "run.log")
    if os.path.exists(logp):
        try:
            t["log_size"] = os.path.getsize(logp)
        except Exception:
            pass
    return t


def build_spider_script(task):
    return gen_spider_script(task)


def start_container(task):
    tid = task["id"]
    name = container_name(tid)
    subprocess.run(["sudo", "docker", "rm", "-f", name], capture_output=True, text=True, timeout=30)
    log_cmd = f"/app/.venv/bin/python /work/tasks/{tid}/spider.py > /work/tasks/{tid}/run.log 2>&1"
    cmd = DOCKER_BASE + ["--name", name, "pyd4vinci/scrapling", "-c", log_cmd]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        raise HTTPException(status_code=502, detail=f"启动容器失败: {r.stderr[:500] or r.stdout[:500]}")
    return name


def stop_container(tid, signal="INT"):
    name = container_name(tid)
    r = subprocess.run(["sudo", "docker", "kill", "-s", signal, name],
                       capture_output=True, text=True, timeout=30)
    return r.returncode == 0


@app.get("/api/tasks")
def api_tasks():
    items = []
    if os.path.isdir(TASKS_DIR):
        for d in sorted(os.listdir(TASKS_DIR), reverse=True):
            t = load_task(d)
            if t:
                items.append(enrich_task(t))
    return {"tasks": items, "types": TASK_TYPES}


@app.post("/api/tasks")
async def api_create_task(request: Request):
    require_quota(request)
    body = await request.json()
    ttype = body.get("type", "crawl")
    if ttype not in TASK_TYPES:
        raise HTTPException(status_code=400, detail=f"未知任务类型: {ttype}")
    name = (body.get("name") or "").strip() or f"任务-{ttype}"
    urls_raw = body.get("start_urls") or ""
    start_urls = [u.strip() for u in urls_raw.replace("\n", ",").split(",") if u.strip()]
    target_website = (body.get("target_website") or "").strip()
    if ttype == "shopify":
        if not target_website and not start_urls:
            raise HTTPException(status_code=400, detail="Shopify 任务请填商店域名或 URL")
    elif not start_urls:
        raise HTTPException(status_code=400, detail="请至少填写一个起始 URL")

    allowed_raw = body.get("allowed_domains") or ""
    allowed = [d.strip() for d in allowed_raw.replace("\n", ",").split(",") if d.strip()]
    if not allowed:
        for u in start_urls:
            m = re.match(r"https?://([^/]+)", u)
            if m:
                allowed.append(m.group(1))
        allowed = list(dict.fromkeys(allowed))

    # 字段定义：支持 "字段名:选择器"（默认CSS）与 "字段名:模式:值"（css/xpath/regex/text/attr）
    fields = []
    frows = [x.strip() for x in (body.get("fields") or "").splitlines() if x.strip()]
    for row in frows:
        parts = [p.strip() for p in row.split(":", 2)]
        fname = parts[0]
        if len(parts) >= 3 and parts[1] in ("css", "xpath", "regex", "text", "attr"):
            mode, val = parts[1], parts[2]
            if mode == "attr":
                seg = val.split("|", 1)
                fields.append({"name": fname, "mode": "attr", "attr": seg[0],
                               "selector": seg[1] if len(seg) > 1 else ""})
            elif mode == "regex":
                seg = val.split("|", 1)
                fields.append({"name": fname, "mode": "regex", "pattern": seg[0],
                               "selector": seg[1] if len(seg) > 1 else ""})
            elif mode == "text":
                fields.append({"name": fname, "mode": "text", "text": val})
            else:
                fields.append({"name": fname, "mode": mode, "selector": val})
        elif len(parts) >= 2 and ":" in row:
            fields.append({"name": fname, "selector": parts[1]})
        else:
            fields.append({"name": fname, "selector": ""})

    rules = body.get("crawl_rules") or {}
    if isinstance(rules, str):
        rules = {"allow": [x.strip() for x in rules.splitlines() if x.strip()]}

    tid = "t" + time.strftime("%y%m%d%H%M%S") + uuid.uuid4().hex[:4]
    task = {
        "id": tid,
        "name": name,
        "type": ttype,
        "start_urls": start_urls,
        "target_website": target_website,
        "allowed_domains": allowed,
        "fields": fields,
        "crawl_rules": rules,
        "itertag": (body.get("itertag") or "item").strip(),
        "csv_headers": [h.strip() for h in (body.get("csv_headers") or "").split(",") if h.strip()],
        "concurrency": int(body.get("concurrency", 4) or 4),
        "download_delay": float(body.get("download_delay", 0.0) or 0.0),
        "autothrottle": bool(body.get("autothrottle", False)),
        "robots_txt_obey": bool(body.get("robots_txt_obey", False)),
        "adaptive": bool(body.get("adaptive", False)),
        "development_mode": bool(body.get("development_mode", False)),
        "proxy_rotation": bool(body.get("proxy_rotation", False)),
        "export_format": (body.get("export_format") or "jsonl").strip(),
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    os.makedirs(task_dir(tid), exist_ok=True)
    save_task(task)
    script = build_spider_script(task)
    with open(os.path.join(task_dir(tid), "spider.py"), "w", encoding="utf-8") as f:
        f.write(script)
    return {"ok": True, "task": enrich_task(task), "script_preview": script[:2000]}


@app.get("/api/tasks/{tid}")
def api_task_detail(tid: str):
    t = load_task(tid)
    if not t:
        raise HTTPException(status_code=404, detail="任务不存在")
    return enrich_task(t)


@app.post("/api/tasks/{tid}/start")
def api_task_start(request: Request, tid: str):
    require_quota(request)
    t = load_task(tid)
    if not t:
        raise HTTPException(status_code=404, detail="任务不存在")
    cur = docker_status(tid)
    if cur == "running":
        raise HTTPException(status_code=400, detail="任务已在运行")
    start_container(t)
    t["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_task(t)
    return {"ok": True, "task": enrich_task(t)}


@app.post("/api/tasks/{tid}/pause")
def api_task_pause(request: Request, tid: str):
    require_quota(request)
    t = load_task(tid)
    if not t:
        raise HTTPException(status_code=404, detail="任务不存在")
    if docker_status(tid) != "running":
        raise HTTPException(status_code=400, detail="任务未在运行")
    ok = stop_container(tid, "INT")
    t["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_task(t)
    return {"ok": ok, "task": enrich_task(t)}


@app.post("/api/tasks/{tid}/resume")
def api_task_resume(request: Request, tid: str):
    require_quota(request)
    t = load_task(tid)
    if not t:
        raise HTTPException(status_code=404, detail="任务不存在")
    if docker_status(tid) == "running":
        raise HTTPException(status_code=400, detail="任务已在运行")
    start_container(t)
    t["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_task(t)
    return {"ok": True, "task": enrich_task(t)}


@app.delete("/api/tasks/{tid}")
def api_task_delete(tid: str):
    t = load_task(tid)
    if not t:
        raise HTTPException(status_code=404, detail="任务不存在")
    stop_container(tid, "KILL")
    subprocess.run(["sudo", "docker", "rm", "-f", container_name(tid)], capture_output=True, text=True, timeout=30)
    subprocess.run(["sudo", "rm", "-rf", task_dir(tid)], capture_output=True, text=True, timeout=30)
    return {"ok": True}


@app.get("/api/tasks/{tid}/log")
def api_task_log(tid: str, tail: int = 200):
    logp = os.path.join(task_dir(tid), "run.log")
    if not os.path.exists(logp):
        return {"log": ""}
    try:
        with open(logp, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return {"log": "".join(lines[-tail:])}
    except Exception as e:
        return {"log": "", "error": str(e)}


@app.get("/api/tasks/{tid}/export")
def api_task_export(tid: str):
    t = load_task(tid)
    if not t:
        raise HTTPException(status_code=404, detail="任务不存在")
    fmt = t.get("export_format", "jsonl")
    items_fp = os.path.join(task_dir(tid), "items.jsonl")
    items = []
    if os.path.exists(items_fp):
        with open(items_fp, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except Exception:
                    continue
    base = f"{tid}_{t.get('name', 'task')}"
    if fmt == "json":
        return JSONResponse(content=items)
    if fmt == "xml":
        rows = []
        for it in items:
            cells = "".join(f"<field name=\"{_xml(k)}\">{_xml(v)}</field>" for k, v in it.items())
            rows.append(f"  <item>{cells}</item>")
        xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<items>\n" + "\n".join(rows) + "\n</items>"
        return Response(content=xml, media_type="application/xml",
                        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(base + '.xml')}"})
    if fmt == "xlsx":
        return Response(content=items_to_xlsx_bytes(items, base), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(base + '.xlsx')}"})
    if fmt == "csv":
        cols = []
        for it in items:
            for k in it.keys():
                if k not in cols:
                    cols.append(k)
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for it in items:
            w.writerow(it)
        return Response(content="\ufeff" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(base + '.csv')}"})
    # jsonl 默认
    content = ""
    if os.path.exists(items_fp):
        with open(items_fp, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    return Response(content=content, media_type="application/x-ndjson",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(base + '.jsonl')}"})


def _xml(v):
    s = "" if v is None else str(v)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")


# ---------------- 会话管理 ----------------

SESSION_PRESETS = {
    "默认": {},
    "桌面Chrome": {"useragent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36", "timezone_id": "Asia/Shanghai", "locale": "zh-CN"},
    "桌面Safari": {"useragent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15", "timezone_id": "America/New_York", "locale": "en-US"},
    "移动iPhone": {"useragent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1", "timezone_id": "Asia/Tokyo", "locale": "ja-JP"},
    "移动安卓": {"useragent": "Mozilla/5.0 (Linux; Android 14; Pixel 8 Build/UD1A.230803.041) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36", "timezone_id": "Asia/Shanghai", "locale": "zh-CN"},
    "硬核反爬": {"hide_canvas": True, "block_webrtc": True, "headless": True},
}

@app.post("/api/sessions")
async def api_session_create(request: Request):
    """创建持久抓取会话（浏览器/反爬/HTTP 三类），支持指纹预设、CDP 远程浏览器、代理"""
    body = await request.json()
    session_type = (body.get("session_type") or "stealthy").strip()
    session_id = (body.get("session_id") or uuid.uuid4().hex[:8]).strip()[:64]
    preset_name = (body.get("preset") or "默认")
    preset = SESSION_PRESETS.get(preset_name, {})
    args = {"session_type": session_type, "session_id": session_id}
    for k in ("headless", "real_chrome", "timezone_id", "locale", "useragent", "proxy",
              "cdp_url", "executable_path", "cookies", "hide_canvas", "block_webrtc", "allow_webgl"):
        if body.get(k) is not None:
            args[k] = body[k]
    for k, v in preset.items():
        if k not in args or args[k] is None:
            args[k] = v
    tool = "open_session" if session_type in ("browser", "stealthy") else "open_request_session"
    try:
        result, _ = mcp_request("tools/call", {"name": tool, "arguments": args})
        return {"ok": True, "session": {"session_id": session_id, "session_type": session_type,
                                         "preset": preset_name, "proxy": args.get("proxy"),
                                         "cdp_url": args.get("cdp_url")}, "detail": result}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"创建会话失败: {e}")

@app.get("/api/sessions")
def api_sessions():
    try:
        result, _ = mcp_request("tools/call", {"name": "list_sessions", "arguments": {}})
        return {"sessions": result}
    except Exception as e:
        return {"sessions": None, "error": str(e)}


@app.post("/api/sessions/{sid}/close")
def api_session_close(sid: str):
    try:
        result, _ = mcp_request("tools/call", {"name": "close_session", "arguments": {"session_id": sid}})
        return {"ok": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"关闭会话失败: {e}")

# ---------------- 页面 ----------------

@app.get("/", response_class=HTMLResponse)
def index():
    try:
        with open("/opt/scrapling-web/index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except Exception:
        return HTMLResponse("<h1>Scrapling Web Console</h1><p>index.html 未找到</p>")


@app.get("/api-docs", response_class=HTMLResponse)
def api_docs():
    """免登录公开 API 文档页"""
    try:
        with open("/opt/scrapling-web/api_docs.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except Exception:
        return HTMLResponse("<h1>API 文档</h1><p>api_docs.html 未找到</p>")

@app.get("/download/csv")
def download_csv():
    if os.path.exists(PROXY_CSV):
        return FileResponse(PROXY_CSV, filename="proxies.csv", media_type="text/csv")
    raise HTTPException(status_code=404, detail="CSV 不存在")

@app.get("/download/json")
def download_json():
    if os.path.exists(PROXY_JSON):
        return FileResponse(PROXY_JSON, filename="proxies.json", media_type="application/json")
    raise HTTPException(status_code=404, detail="JSON 不存在")

@app.get("/download/residential-client.py")
def download_residential_client():
    """住宅代理客户端脚本下载（设备上运行，上报住宅节点）"""
    path = "/opt/scrapling-web/residential_client.py"
    if os.path.exists(path):
        return FileResponse(path, filename="residential_client.py", media_type="text/plain")
    raise HTTPException(status_code=404, detail="客户端脚本不存在")

# ---------------- MCP Server 挂载（/mcp，供 AI agent 链接调用） ----------------
# 整个控制台暴露为 MCP 工具（代理池/存活池/代理访问/体检/爬虫任务/会话/Scrapling 抓取），
# 客户端用 streamable HTTP 连接 http://<host>:8080/mcp，带 Authorization: Bearer <MCP_TOKEN>


# ---------------- 用户 / 额度 / API Key / 管理 ----------------

def _user_public(user: dict) -> dict:
    return {
        "id": user.get("id"), "username": user.get("username"),
        "role": user.get("role"), "quota": user.get("quota", 0),
        "total_quota": user.get("total_quota", 0),
        "status": user.get("status", "active"),
        "master": user.get("role") == "master",
    }

@app.post("/api/auth/register")
async def api_register(request: Request):
    body = await request.json()
    email = (body.get("email") or body.get("username") or "").strip().lower()
    password = body.get("password") or ""
    if not auth.is_valid_email(email):
        raise HTTPException(status_code=400, detail="请输入有效的邮箱地址")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="密码至少 6 位")
    if auth.get_user_by_username(email):
        raise HTTPException(status_code=409, detail="该邮箱已注册，请直接登录")
    user = auth.create_user(email, password)
    token = auth.create_session(user["id"])
    # 注册后发送欢迎邮件（未配置 SMTP 时自动跳过，不影响注册）
    auth.send_welcome_email(email)
    return {"token": token, "user": _user_public(user)}

@app.post("/api/auth/login")
async def api_login(request: Request):
    body = await request.json()
    account = (body.get("account") or body.get("email") or body.get("username") or "").strip().lower()
    user = auth.login(account, body.get("password") or "")
    if not user:
        raise HTTPException(status_code=401, detail="邮箱或密码错误")
    if isinstance(user, dict) and user.get("error"):
        raise HTTPException(status_code=403, detail=user["error"])
    token = auth.create_session(user["id"])
    return {"token": token, "user": _user_public(user)}

@app.post("/api/auth/logout")
async def api_logout(request: Request):
    st = request.headers.get("X-Session-Token", "")
    if st:
        auth.delete_session(st)
    return {"ok": True}

@app.get("/api/me")
def api_me(request: Request):
    user = current_user(request)
    usage = auth.get_usage(user["id"], 30) if user.get("role") != "master" else []
    return {"user": _user_public(user), "usage": usage}

@app.post("/api/me/password")
async def api_change_password(request: Request):
    user = current_user(request)
    if user.get("role") == "master":
        raise HTTPException(status_code=400, detail="主人通道无需修改密码")
    body = await request.json()
    if not auth.verify_password(body.get("old") or "", user["password_hash"]):
        raise HTTPException(status_code=400, detail="原密码错误")
    new_pw = body.get("new") or ""
    if len(new_pw) < 6:
        raise HTTPException(status_code=400, detail="新密码至少 6 位")
    auth.update_user(user["id"], password_hash=auth.hash_password(new_pw))
    return {"ok": True}

@app.get("/api/me/apikeys")
def api_list_apikeys(request: Request):
    user = current_user(request)
    if user.get("role") == "master":
        return {"keys": [], "hint": "主人通道请使用平台级 MCP Bearer 令牌"}
    return {"keys": auth.list_api_keys(user["id"])}

@app.post("/api/me/apikeys")
async def api_create_apikey(request: Request):
    user = current_user(request)
    if user.get("role") == "master":
        raise HTTPException(status_code=400, detail="主人通道无需创建 API Key")
    body = await request.json()
    k = auth.create_api_key(user["id"], (body.get("name") or "默认").strip())
    return {"key": k["key"], "id": k["id"]}

@app.delete("/api/me/apikeys/{kid}")
def api_delete_apikey(kid: str, request: Request):
    user = current_user(request)
    if user.get("role") == "master":
        return {"ok": True}
    auth.delete_api_key(kid, user["id"])
    return {"ok": True}

@app.get("/api/me/usage")
def api_my_usage(request: Request, limit: int = 50):
    user = current_user(request)
    return {"usage": auth.get_usage(user["id"], min(limit, 200))}

@app.get("/api/settings-public")
def api_settings_public():
    return auth.public_settings()

@app.get("/api/stats-public")
def api_stats_public():
    """首页公开指标（无需登录）：代理池/存活池/工具数"""
    try:
        proxies = len(load_proxies())
    except Exception:
        proxies = 0
    try:
        alive = len(load_alive())
    except Exception:
        alive = 0
    mcp_tools = 0
    try:
        m, _ = mcp_request("tools/list")
        if m and m.get("tools"):
            mcp_tools = len(m["tools"])
    except Exception:
        pass
    return {"proxies": proxies, "alive": alive, "mcp_tools": mcp_tools}

@app.get("/api/scrapling/advanced/tools")
def api_advanced_tools(request: Request):
    """列出高级工具（页面转 Markdown/全站 Markdown/XHR 捕获/选择器生成/CLI 提取）"""
    require_quota(request)
    return {"tools": [
        {"name": "page_markdown", "desc": "把网页转成 LLM 就绪的干净 Markdown", "args": ["url", "css_selector?"]},
        {"name": "site_markdown", "desc": "把整个网站爬成 Markdown 语料库", "args": ["url", "max_pages?"]},
        {"name": "capture_xhr", "desc": "浏览器加载页面并捕获其 API 请求（XHR/fetch）", "args": ["url"]},
        {"name": "selector_gen", "desc": "在网页上验证 CSS/XPath/文本选择器", "args": ["url", "css?|xpath?|text?"]},
        {"name": "extract_cli", "desc": "CLI extract 等价：一键提取 md/txt/html", "args": ["url", "format?", "css_selector?", "mode?"]},
    ]}

@app.post("/api/scrapling/advanced")
async def api_scrapling_advanced(request: Request):
    """执行高级工具（在 pyd4vinci/scrapling 容器内跑 advanced_tools.py）"""
    require_quota(request)
    body = await request.json()
    tool = (body.get("tool") or "").strip()
    args = body.get("args") or {}
    if tool not in ("page_markdown", "site_markdown", "capture_xhr", "selector_gen", "extract_cli"):
        raise HTTPException(status_code=400, detail=f"未知高级工具: {tool}")
    try:
        job = json.dumps({"tool": tool, "args": args}, ensure_ascii=False)
        r = subprocess.run(
            ["sudo", "docker", "run", "--rm", "--entrypoint", "/app/.venv/bin/python",
             "-v", "/opt/scrapling:/work", "pyd4vinci/scrapling",
             "/work/advanced_tools.py", job],
            capture_output=True, text=True, timeout=280)
        out = (r.stdout or "").strip()
        if not out:
            return {"ok": False, "error": f"容器无输出: {(r.stderr or '')[:300]}"}
        try:
            return json.loads(out)
        except Exception:
            return {"ok": False, "error": f"无法解析输出: {out[:400]}"}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="高级工具执行超时（>280 秒）")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"高级工具执行失败: {e}")


# ---------------- 管理员 ----------------

@app.get("/api/admin/users")
def admin_users(request: Request):
    require_admin(request)
    return {"users": auth.list_users()}

@app.post("/api/admin/users")
async def admin_create_user(request: Request):
    require_admin(request)
    body = await request.json()
    email = (body.get("email") or body.get("username") or "").strip().lower()
    password = body.get("password") or ""
    if not auth.is_valid_email(email):
        raise HTTPException(status_code=400, detail="请输入有效的邮箱地址")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="密码至少 6 位")
    if auth.get_user_by_username(email):
        raise HTTPException(status_code=409, detail="该邮箱已存在")
    quota = int(body.get("quota") or auth.get_setting("trial_quota") or 0)
    role = "admin" if body.get("role") == "admin" else "user"
    user = auth.create_user(email, password, role=role, quota=quota)
    return {"user": _user_public(user)}

@app.post("/api/admin/users/{uid}/quota")
async def admin_add_quota(uid: str, request: Request):
    require_admin(request)
    body = await request.json()
    n = int(body.get("n") or 0)
    if n <= 0:
        raise HTTPException(status_code=400, detail="额度必须为正数")
    auth.add_quota(uid, n)
    return {"ok": True, "user": _user_public(auth.get_user_by_id(uid))}

@app.post("/api/admin/users/{uid}/status")
async def admin_set_status(uid: str, request: Request):
    require_admin(request)
    body = await request.json()
    status = body.get("status")
    if status not in ("active", "disabled"):
        raise HTTPException(status_code=400, detail="status 只能是 active/disabled")
    auth.update_user(uid, status=status)
    return {"ok": True}

@app.post("/api/admin/users/{uid}/role")
async def admin_set_role(uid: str, request: Request):
    require_admin(request)
    body = await request.json()
    role = body.get("role")
    if role not in ("user", "admin"):
        raise HTTPException(status_code=400, detail="role 只能是 user/admin")
    auth.update_user(uid, role=role)
    return {"ok": True}

@app.get("/api/admin/settings")
def admin_get_settings(request: Request):
    require_admin(request)
    return {"settings": auth.get_all_settings()}

@app.post("/api/admin/settings")
async def admin_set_settings(request: Request):
    require_admin(request)
    body = await request.json()
    for k, v in body.items():
        if k in auth.DEFAULT_SETTINGS:
            auth.set_setting(k, str(v))
    return {"settings": auth.get_all_settings()}

@app.get("/api/admin/stats")
def admin_stats(request: Request):
    require_admin(request)
    return auth.stats()

@app.post("/api/admin/test-email")
async def admin_test_email(request: Request):
    """用当前 SMTP 配置发送一封测试邮件"""
    require_admin(request)
    body = await request.json()
    to = (body.get("to") or "").strip()
    if not auth.is_valid_email(to):
        raise HTTPException(status_code=400, detail="请输入有效的收件邮箱")
    res = auth.send_email(to, "Scrapling 云抓取平台 - SMTP 测试邮件",
                          "这是一封来自 Scrapling 云抓取平台的测试邮件。\n\n如果收到此邮件，说明发信邮箱配置正确。")
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail="发送失败：" + res.get("error", "未知错误"))
    return {"ok": True}

try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import mcp_server  # noqa: E402
    from contextlib import asynccontextmanager

    _mcp_asgi = mcp_server.mcp.http_app(path="/")

    @asynccontextmanager
    async def _combined_lifespan(application):
        async with _mcp_asgi.lifespan(application):
            yield

    app.mount("/mcp", _mcp_asgi)
    app.router.lifespan_context = _combined_lifespan
    print("MCP server mounted at /mcp")
except Exception as e:
    print("MCP mount failed:", e)

threading.Thread(target=_paid_source_loop, daemon=True).start()
print("paid-source loop started")
