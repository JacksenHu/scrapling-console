# -*- coding: utf-8 -*-
"""
advanced_tools.py - Scrapling 高级工具执行器（在 pyd4vinci/scrapling 容器内运行）
用法: python advanced_tools.py '<json>'
json = {"tool": "page_markdown", "args": {...}}
工具: page_markdown / site_markdown / capture_xhr / selector_gen / extract_cli
"""
import json
import sys


def fail(msg):
    print(json.dumps({"ok": False, "error": str(msg)[:500]}, ensure_ascii=False))
    sys.exit(0)


def page_markdown(args):
    from scrapling.fetchers import Fetcher
    url = args["url"]
    css = (args.get("css_selector") or "").strip()
    page = Fetcher.get(url, impersonate=args.get("impersonate", "chrome"), stealthy_headers=True)
    if css:
        nodes = page.css(css)
        md = "\n\n".join(n.markdown() for n in nodes[:50])
        return {"ok": True, "source": url, "selector": css, "matched": len(nodes),
                "markdown": md[:20000]}
    md = page.markdown()
    return {"ok": True, "source": url, "markdown": md[:20000]}


def site_markdown(args):
    from scrapling.spiders import SiteToMarkdownSpider
    url = args["url"]
    max_pages = int(args.get("max_pages", 20) or 20)

    class S(SiteToMarkdownSpider):
        start_urls = [url]
        max_pages = max_pages

    result = S().start()
    items = list(result.items)[:10]
    text = "\n\n---\n\n".join(str(i) for i in items)
    return {"ok": True, "source": url, "pages_crawled": len(result.items),
            "markdown_preview": text[:20000]}


def capture_xhr(args):
    from scrapling.fetchers import DynamicFetcher
    url = args["url"]
    page = DynamicFetcher.fetch(url, headless=True, network_idle=True,
                                disable_resources=False, capture_xhr="*")
    xs = page.captured_xhr or []
    out = []
    for x in xs[:60]:
        try:
            ct = (x.headers or {}).get("content-type", "")
            out.append({"url": getattr(x, "url", ""), "status": getattr(x, "status", ""),
                        "type": ct})
        except Exception:
            pass
    return {"ok": True, "source": url, "captured_count": len(xs), "captured": out}


def selector_gen(args):
    from scrapling.fetchers import Fetcher
    url = args["url"]
    page = Fetcher.get(url, impersonate="chrome", stealthy_headers=True)
    if (args.get("css") or "").strip():
        els = page.css(args["css"].strip())
        samples = [e.text[:80] for e in els[:5]]
        return {"ok": True, "method": "css", "selector": args["css"],
                "count": len(els), "samples": samples}
    if (args.get("xpath") or "").strip():
        els = page.xpath(args["xpath"].strip())
        samples = [e.text[:80] for e in els[:5]]
        return {"ok": True, "method": "xpath", "selector": args["xpath"],
                "count": len(els), "samples": samples}
    if (args.get("text") or "").strip():
        els = page.find_by_text(args["text"].strip())
        samples = [e.text[:80] for e in els[:5]]
        return {"ok": True, "method": "text", "selector": args["text"],
                "count": len(els), "samples": samples}
    return {"ok": True, "hint": "请提供 css / xpath / text 三者之一"}


def extract_cli(args):
    from scrapling.fetchers import Fetcher, StealthyFetcher, DynamicFetcher
    url = args["url"]
    fmt = (args.get("format") or "md").lower()
    css = (args.get("css_selector") or "").strip()
    mode = (args.get("mode") or "get").lower()

    if mode == "stealthy-fetch":
        page = StealthyFetcher.fetch(url, headless=True, network_idle=True, solve_cloudflare=True)
    elif mode == "fetch":
        page = DynamicFetcher.fetch(url, headless=True, network_idle=True)
    else:
        page = Fetcher.get(url, impersonate=args.get("impersonate", "chrome"),
                           stealthy_headers=True)

    nodes = page.css(css) if css else None
    if fmt == "txt":
        content = "\n".join(n.text for n in nodes[:100]) if nodes else page.body[0].text
    elif fmt == "html":
        content = "\n".join(str(n.html) for n in nodes[:50]) if nodes else page.html_content
    else:
        content = "\n\n".join(n.markdown() for n in nodes[:50]) if nodes else page.markdown()
    return {"ok": True, "source": url, "format": fmt, "selector": css or "(整页)",
            "content": content[:20000]}


TOOLS = {
    "page_markdown": page_markdown,
    "site_markdown": site_markdown,
    "capture_xhr": capture_xhr,
    "selector_gen": selector_gen,
    "extract_cli": extract_cli,
}


def main():
    if len(sys.argv) < 2:
        fail("缺少参数")
    try:
        job = json.loads(sys.argv[1])
    except Exception as e:
        fail(f"参数不是合法 JSON: {e}")
    tool = job.get("tool", "")
    args = job.get("args", {}) or {}
    fn = TOOLS.get(tool)
    if not fn:
        fail(f"未知工具: {tool}，可用: {list(TOOLS)}")
    try:
        print(json.dumps(fn(args), ensure_ascii=False))
    except Exception as e:
        fail(f"执行失败: {e}")


if __name__ == "__main__":
    main()
