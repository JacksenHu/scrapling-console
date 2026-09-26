#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Scrapling 增强快捷工具（容器内执行）
用法: python quick_tool.py <config.json>
config:
{
  "action": "fetch" | "links",
  "url": "...",
  "css_selector": "...",
  "extraction_type": "markdown"|"text",
  "adaptive": true|false,      # 自适应选择器
  "use_proxy": true|false,     # 轮换代理池
  "dev_cache": true|false,     # 开发模式缓存（本地缓存重放）
  "mode": "simple"|"stealthy", # 抓取模式
  "main_content_only": true|false,
  "timeout": 30
}
输出: stdout JSON
"""
import json
import os
import sys
import hashlib
import random
import time

from scrapling.fetchers import Fetcher, StealthyFetcher, ProxyRotator

CACHE_DIR = "/work/.cache"
PROXIES_FILE = "/work/proxies.json"

def load_proxies(n=10):
    try:
        with open(PROXIES_FILE, encoding="utf-8") as f:
            pool = json.load(f)
        picks = random.sample(pool, min(n, len(pool)))
        out = []
        for p in picks:
            if p.get("ip") and p["ip"] != "0.0.0.0" and p.get("port"):
                out.append(f"{p.get('protocol', 'http')}://{p['ip']}:{p['port']}")
        return out
    except Exception:
        return []

def cache_path(url, mode):
    os.makedirs(CACHE_DIR, exist_ok=True)
    h = hashlib.md5(f"{mode}:{url}".encode()).hexdigest()
    return os.path.join(CACHE_DIR, h + ".json")

def read_cache(url, mode):
    fp = cache_path(url, mode)
    if os.path.exists(fp):
        try:
            return json.load(open(fp, encoding="utf-8"))
        except Exception:
            return None
    return None

def write_cache(url, mode, data):
    fp = cache_path(url, mode)
    try:
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass

def do_fetch(cfg):
    url = cfg["url"]
    mode = cfg.get("mode", "simple")
    adaptive = bool(cfg.get("adaptive"))
    use_proxy = bool(cfg.get("use_proxy"))
    dev_cache = bool(cfg.get("dev_cache"))
    timeout = int(cfg.get("timeout", 30))

    if dev_cache:
        cached = read_cache(url, mode)
        if cached:
            cached["_cache"] = "hit"
            return cached

    proxies = load_proxies() if use_proxy else []

    if mode == "stealthy":
        fetcher = StealthyFetcher()
        try:
            resp = fetcher.fetch(url, timeout=timeout)
        except Exception as e:
            return {"ok": False, "error": f"stealthy fetch failed: {e}"}
    else:
        fetcher = Fetcher()
        if not proxies:
            try:
                resp = fetcher.get(url, timeout=timeout)
            except Exception as e:
                return {"ok": False, "error": f"fetch failed: {e}"}
        else:
            # 代理轮换：依次尝试最多 4 个代理，直到成功
            last_err = "no proxy tried"
            ok = False
            for proxy in proxies[:4]:
                try:
                    resp = fetcher.get(url, timeout=min(timeout, 15), proxy=proxy)
                    ok = True
                    break
                except Exception as e:
                    last_err = f"{str(e)[:120]}"
            if not ok:
                return {"ok": False, "error": f"所有代理均失败: {last_err}"}

    status = getattr(resp, "status", 0)
    body_bytes = getattr(resp, "body", b"") or b""
    try:
        text = body_bytes.decode("utf-8", errors="replace")
    except Exception:
        text = ""
    if not text and hasattr(resp, "text"):
        try:
            text = resp.text or ""
        except Exception:
            text = ""

    result = {"ok": True, "status": status, "url": url, "text": text[:200000]}

    # 提取
    css = cfg.get("css_selector") or ""
    if css:
        try:
            nodes = resp.css(css, adaptive=adaptive)
            items = []
            for n in nodes[:50]:
                item = {"text": n.css("::text").get() or ""}
                for attr in ("href", "src", "alt", "title", "class", "id"):
                    try:
                        v = n.attr(attr)
                        if v:
                            item[attr] = v
                    except Exception:
                        pass
                items.append(item)
            result["items"] = items
            result["item_count"] = len(items)
        except Exception as e:
            result["extract_error"] = str(e)

    # 链接提取
    if cfg.get("extract_links"):
        try:
            from scrapling.spiders import LinkExtractor
            le = LinkExtractor()
            links = le.extract(resp)
            seen = set()
            out = []
            for l in links[:500]:
                u = str(l)
                if u not in seen:
                    seen.add(u)
                    out.append(u)
            result["links"] = out
            result["link_count"] = len(out)
        except Exception as e:
            result["link_error"] = str(e)

    if dev_cache:
        write_cache(url, mode, result)
    return result

def do_links(cfg):
    url = cfg["url"]
    mode = cfg.get("mode", "simple")
    result = do_fetch({**cfg, "extract_links": True})
    return {"ok": result.get("ok", False),
            "url": url,
            "status": result.get("status"),
            "links": result.get("links", []),
            "link_count": result.get("link_count", 0),
            "error": result.get("error")}

def main():
    cfg_file = sys.argv[1]
    with open(cfg_file, encoding="utf-8") as f:
        cfg = json.load(f)
    action = cfg.get("action", "fetch")
    try:
        if action == "links":
            out = do_links(cfg)
        else:
            out = do_fetch(cfg)
    except Exception as e:
        out = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    print(json.dumps(out, ensure_ascii=False))

if __name__ == "__main__":
    main()
