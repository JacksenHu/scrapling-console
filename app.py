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

def pick_proxy_for(url):
    """选代理优先级：指定代理 > 该站可用池 > 存活代理池 > 全池随机"""
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
    proxy: 可选指定代理（ip:port 或 http://ip:port），不填则自动随机
    retries: 失败自动换代理重试次数（默认 1）
    """
    require_quota(request)
    body = await request.json()
    url = (body.get("url") or "").strip()
    mode = body.get("mode", "plain")
    specified = (body.get("proxy") or "").strip()
    timeout = min(max(int(body.get("timeout", 30) or 30), 5), 120)
    retries = max(1, min(int(body.get("retries", 1) or 1), 5))
    if not url:
        raise HTTPException(status_code=400, detail="请填写网址")
    if mode not in ("plain", "browser", "screenshot"):
        raise HTTPException(status_code=400, detail=f"未知模式: {mode}")

    proxy_url = None
    if specified:
        proxy_url = specified if "://" in specified else f"http://{specified}"

    last_err = "无可用代理"
    last_proxy = None
    for attempt in range(retries):
        if proxy_url is None:
            proxy_url = pick_proxy_for(url)
        if not proxy_url:
            raise HTTPException(status_code=502, detail="代理池为空，请先在代理池页爬取代理")
        last_proxy = proxy_url
        t0 = time.time()
        exit_ip = check_exit_ip(proxy_url, timeout=6)
        if exit_ip == "LEAK":
            # 透明代理：出口=服务器IP，等于裸奔，必须换
            last_err = f"代理 {proxy_url} 是透明代理（出口=服务器IP，会暴露真实IP），已自动换下一个"
            if attempt + 1 < retries:
                proxy_url = None
                continue
            log_proxy_visit({"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "url": url, "mode": mode,
                             "proxy": proxy_url, "exit_ip": None,
                             "ms": int((time.time() - t0) * 1000), "ok": False, "error": last_err})
            raise HTTPException(status_code=502, detail=last_err)
        # exit_ip 为 None = 回显服务不可达（出口未知），不判死，直接试抓
        try:
            if mode == "plain":
                # 普通请求：服务器 requests 直接走代理（与目标站体检同路径，兼容免费代理）
                r = requests.get(url, proxies={"http": proxy_url, "https": proxy_url},
                                 timeout=min(timeout, 25),
                                 headers={"User-Agent": "Mozilla/5.0"},
                                 allow_redirects=True)
                text = r.text[:60000]
                result = {"type": "text",
                          "text": f"[HTTP {r.status_code}] 页面大小 {len(r.content)} 字节\n\n{text}"}
            elif mode == "browser":
                sess = call_tool("open_session", {
                    "session_type": "dynamic", "headless": True, "proxy": proxy_url})
                session_id = extract_session_id(sess)
                try:
                    # 先验证浏览器出口：明确泄漏(出口=服务器IP)才丢弃；回显不通不判死
                    probe = call_tool("session_fetch", {
                        "url": ECHO_SERVICE, "session_id": session_id,
                        "main_content_only": False, "timeout": 25000})
                    probe_ip = extract_ip_from_text(mcp_text(probe))
                    srv = get_server_ip()
                    if probe_ip and srv and probe_ip == srv:
                        raise Exception(f"浏览器代理未生效（出口=服务器IP，会暴露），已丢弃该代理")
                    if probe_ip and not exit_ip:
                        exit_ip = probe_ip
                    result = parse_result_content(call_tool("session_fetch", {
                        "url": url, "session_id": session_id, "network_idle": True,
                        "main_content_only": False, "timeout": 60000}))
                finally:
                    try:
                        call_tool("close_session", {"session_id": session_id})
                    except Exception:
                        pass
            else:  # screenshot
                sess = call_tool("open_session", {
                    "session_type": "dynamic", "headless": True, "proxy": proxy_url})
                session_id = extract_session_id(sess)
                try:
                    # 同样先验证浏览器出口：明确泄漏才丢弃
                    probe = call_tool("session_fetch", {
                        "url": ECHO_SERVICE, "session_id": session_id,
                        "main_content_only": False, "timeout": 25000})
                    probe_ip = extract_ip_from_text(mcp_text(probe))
                    srv = get_server_ip()
                    if probe_ip and srv and probe_ip == srv:
                        raise Exception(f"浏览器代理未生效（出口=服务器IP，会暴露），已丢弃该代理")
                    if probe_ip and not exit_ip:
                        exit_ip = probe_ip
                    result = parse_result_content(call_tool("screenshot", {
                        "url": url, "session_id": session_id, "image_type": "png",
                        "full_page": False, "network_idle": True, "timeout": 60000}))
                finally:
                    try:
                        call_tool("close_session", {"session_id": session_id})
                    except Exception:
                        pass
            ms = int((time.time() - t0) * 1000)
            log_proxy_visit({"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "url": url, "mode": mode,
                             "proxy": proxy_url, "exit_ip": exit_ip, "ms": ms, "ok": True})
            # 真实访问成功 → 回写存活代理池（越用越准）
            try:
                pp = proxy_url.split("://")[-1]
                if ":" in pp:
                    ip, port = pp.rsplit(":", 1)
                    touch_alive(ip, port, proxy_url.split("://")[0], ms, exit_ip or "")
            except Exception:
                pass
            return {"ok": True, "mode": mode, "proxy": proxy_url, "exit_ip": exit_ip,
                    "ms": ms, "attempts": attempt + 1, "result": result}
        except HTTPException:
            raise
        except Exception as e:
            last_err = str(e)[:300]
            if attempt + 1 < retries:
                proxy_url = None
                continue
            log_proxy_visit({"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "url": url, "mode": mode,
                             "proxy": proxy_url, "exit_ip": exit_ip,
                             "ms": int((time.time() - t0) * 1000), "ok": False, "error": last_err})
            raise HTTPException(status_code=502,
                                detail=f"第 {attempt + 1} 次尝试失败（代理 {last_proxy}）: {last_err}")

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

    # 字段定义：支持 "字段名:选择器" 每行一个，或字段 JSON
    fields = []
    frows = [x.strip() for x in (body.get("fields") or "").splitlines() if x.strip()]
    for row in frows:
        if ":" in row:
            fname, sel = [p.strip() for p in row.split(":", 1)]
        else:
            fname, sel = row, ""
        fields.append({"name": fname, "selector": sel})

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
