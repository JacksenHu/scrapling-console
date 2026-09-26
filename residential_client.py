#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""住宅代理客户端：把你的设备出口 IP + 代理端口上报到 Scrapling 云抓取平台，
平台验证通过后入库「住宅节点」，代理访问时优先走你自己的住宅 IP（注册账号风控最优）。

两种接入方式：
  A. 应用内置（第三方应用静默模式）：管理后台创建应用拿到 App Token，内置进你的应用，用户运行即自动上报：
     python residential_client.py --api https://YOUR-SERVER:8080 --app-token <AppToken> --port 3128 --interval 300
  B. 本人自用：控制台「API 接入」页生成 API Key（sk- 开头）：
     python residential_client.py --api https://YOUR-SERVER:8080 --api-key sk-你的APIKey --port 3128 --interval 300

前提（重要）：
  1. 设备已运行代理软件并监听 --port（例如 gost / 3proxy / CCProxy 等），协议默认 http；
  2. 路由器已对该端口做端口映射（公网可访问到该设备），否则平台无法通过该端口访问；
  3. 应用模式用 App Token（X-App-Token），自用模式用 API Key（X-Api-Key）。

程序会自动：探测公网出口 IP → 上报节点 → 后台验证 → 每 --interval 秒心跳保活。
"""
import argparse
import json
import socket
import time
import urllib.error
import urllib.request


def get_public_ip():
    """探测公网出口 IP（多服务容错）"""
    for svc in ("http://members.3322.org/dyndns/getip", "https://api.ipify.org", "https://ifconfig.me/ip"):
        try:
            req = urllib.request.Request(svc, headers={"User-Agent": "residential-client/1.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                ip = r.read().decode().strip().splitlines()[0].strip()
                if ip:
                    return ip
        except Exception:
            continue
    return None


def call(api, key, path, body, token_mode=False, timeout=30):
    data = json.dumps(body).encode()
    hd = {"Content-Type": "application/json"}
    if token_mode:
        hd["X-App-Token"] = key
    else:
        hd["X-Api-Key"] = key
    req = urllib.request.Request(api.rstrip("/") + path, data=data, method="POST", headers=hd)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return {"ok": False, "error": "HTTP %s %s" % (e.code, e.read().decode()[:200])}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def main():
    ap = argparse.ArgumentParser(description="Scrapling 住宅代理客户端")
    ap.add_argument("--api", default="https://YOUR-SERVER:8080", help="平台地址（部署时填写你的服务器地址）")
    ap.add_argument("--api-key", default="", help="自用模式：API Key（控制台-API接入生成，sk- 开头）")
    ap.add_argument("--app-token", default="", help="应用模式：App Token（管理后台-住宅节点池创建）")
    ap.add_argument("--port", type=int, required=True, help="本机代理端口（如 3128 / 8080）")
    ap.add_argument("--interval", type=int, default=300, help="心跳间隔秒，默认 300")
    ap.add_argument("--protocol", default="http", help="代理协议，默认 http")
    args = ap.parse_args()

    if not (args.api_key or args.app_token):
        print("[!] 必须提供 --api-key 或 --app-token 之一")
        return
    token_mode = bool(args.app_token)
    print("住宅代理客户端启动（Ctrl+C 退出）")
    print("平台:", args.api, "| 端口:", args.port, "| 模式:", "应用(App Token)" if token_mode else "自用(API Key)", "| 心跳:", args.interval, "s")
    node_id = None
    while True:
        ip = get_public_ip()
        if not ip:
            print("[!] 无法探测公网出口 IP，10 秒后重试")
            time.sleep(10)
            continue
        if node_id is None:
            key = args.app_token or args.api_key
            r = call(args.api, key, "/api/residential/report",
                     {"ip": ip, "port": args.port, "protocol": args.protocol,
                      "name": socket.gethostname()[:40]}, token_mode=token_mode)
            if r.get("ok"):
                node_id = r["node"]["node_id"]
                print("[+] 已上报 %s:%s 节点 %s（后台验证中，通过后即可用于代理访问）" % (ip, args.port, node_id))
            else:
                print("[!] 上报失败:", r.get("error") or r.get("msg"))
                time.sleep(30)
                continue
        else:
            hb = call(args.api, args.app_token or args.api_key, "/api/residential/heartbeat",
                      {"node_id": node_id}, token_mode=token_mode)
            if not hb.get("ok") or not hb.get("hit"):
                node_id = None
                print("[!] 节点失效，重新上报")
                continue
            print("[.] %s 心跳 OK" % time.strftime("%H:%M:%S"))
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
