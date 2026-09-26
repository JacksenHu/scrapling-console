# -*- coding: utf-8 -*-
"""
auth.py - Scrapling Console 用户/额度/API Key/设置系统
SQLite 存储（标准库实现，无第三方依赖）
"""
import hashlib
import json
import os
import re
import secrets
import smtplib
import sqlite3
import time
import uuid
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr

DB_PATH = os.environ.get("SCRAPLING_AUTH_DB", "/opt/scrapling/app.db")

# 默认设置（可用 /api/admin/settings 修改）
DEFAULT_SETTINGS = {
    "site_name": "Scrapling 云抓取平台",
    "announcement": "欢迎使用 Scrapling 云抓取：网页抓取、反爬绕过、代理访问，一个平台全搞定。",
    "payment_enabled": "0",          # 1=已配置支付(显示购买按钮) 0=未配置(按钮变联系客服)
    "contact_url": "mailto:support@example.com",
    "contact_text": "联系客服",
    "trial_quota": "20",             # 新用户注册赠送的免费额度（次）
    "price_per_quota": "0.05",       # 每 100 次额度的单价（人民币，展示用）
    "quota_package": "500",          # 默认购买套餐（次）
    # 发信邮箱（SMTP）配置
    "smtp_host": "",                 # SMTP 服务器地址，如 smtp.qq.com
    "smtp_port": "465",              # 端口（465=SSL，587=STARTTLS）
    "smtp_user": "",                 # SMTP 账号（邮箱地址）
    "smtp_pass": "",                 # SMTP 授权码/密码
    "smtp_from": "",                 # 发件人显示名称
    "smtp_tls": "1",                 # 1=SSL(465) 0=STARTTLS(587)
    "smtp_welcome": "1",             # 1=注册后发送欢迎邮件 0=不发送
}

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user',   -- user / admin
        quota INTEGER NOT NULL DEFAULT 0,
        total_quota INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'active',  -- active / disabled
        created_at TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS api_keys (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        key TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        last_used_at TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS usage_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        action TEXT NOT NULL,
        cost INTEGER NOT NULL DEFAULT 1,
        ts TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""")
    for k, v in DEFAULT_SETTINGS.items():
        conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)", (k, v))
    conn.commit()
    conn.close()


def hash_password(pw: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), salt.encode("utf-8"), 120000)
    return f"{salt}${h.hex()}"


def verify_password(pw: str, stored: str) -> bool:
    try:
        salt, hx = stored.split("$", 1)
        h = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), salt.encode("utf-8"), 120000)
        return h.hex() == hx
    except Exception:
        return False


def create_user(username: str, password: str, role: str = "user", quota: int = None) -> dict:
    if quota is None:
        quota = int(get_setting("trial_quota") or DEFAULT_SETTINGS["trial_quota"])
    uid = uuid.uuid4().hex[:16]
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users(id, username, password_hash, role, quota, total_quota, status, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (uid, username, hash_password(password), role, quota, quota, "active",
             time.strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        return get_user_by_username(username, conn)
    finally:
        conn.close()


def is_valid_email(s: str) -> bool:
    return bool(s and EMAIL_RE.match(s))


def send_email(to: str, subject: str, body: str) -> dict:
    """发送邮件；SMTP 未配置或发送失败时返回 {'ok': False, 'error': ...}"""
    s = get_all_settings()
    host = s.get("smtp_host", "").strip()
    user = s.get("smtp_user", "").strip()
    pwd = s.get("smtp_pass", "")
    if not host or not user:
        return {"ok": False, "error": "SMTP 未配置"}
    port = int(s.get("smtp_port") or 465)
    use_ssl = s.get("smtp_tls", "1") == "1"
    from_name = s.get("smtp_from", "").strip() or "Scrapling 云抓取平台"
    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port, timeout=15)
        else:
            server = smtplib.SMTP(host, port, timeout=15)
            server.starttls()
        server.login(user, pwd)
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = formataddr((str(Header(from_name, "utf-8")), user))
        msg["To"] = to
        server.sendmail(user, [to], msg.as_string())
        server.quit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def send_welcome_email(email: str) -> dict:
    """注册欢迎邮件；smtp_welcome=0 或未配置时返回 ok=True(跳过)"""
    s = get_all_settings()
    if s.get("smtp_welcome", "1") != "1":
        return {"ok": True, "skipped": True}
    site = s.get("site_name") or "Scrapling 云抓取平台"
    quota = s.get("trial_quota") or "20"
    contact = s.get("contact_text") or "联系客服"
    body = (
        f"欢迎使用 {site}！\n\n"
        f"你的账号已注册成功：{email}\n"
        f"注册即送 {quota} 次免费额度，可用于网页抓取、反爬绕过、代理访问、MCP/AI 调用。\n\n"
        f"控制台地址：https://你的站点/（或管理员提供的地址）\n"
        f"额度用完可联系{contact}充值。\n\n"
        f"祝抓取顺利！\n{site} 团队"
    )
    return send_email(email, f"欢迎使用 {site}", body)


def get_user_by_username(username: str, conn=None):
    own = conn is None
    if own:
        conn = get_db()
    try:
        row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        return dict(row) if row else None
    finally:
        if own:
            conn.close()


def get_user_by_id(uid: str):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def login(account: str, password: str) -> dict:
    """邮箱/用户名 + 密码登录"""
    user = get_user_by_username(account)
    if not user or not verify_password(password, user["password_hash"]):
        return None
    if user["status"] != "active":
        return {"error": "账号已被禁用"}
    return user


def create_session(user_id: str) -> str:
    token = secrets.token_urlsafe(32)
    conn = get_db()
    try:
        conn.execute("INSERT INTO sessions(token, user_id, created_at) VALUES (?,?,?)",
                     (token, user_id, time.strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        return token
    finally:
        conn.close()


def get_session_user(token: str):
    if not token:
        return None
    conn = get_db()
    try:
        row = conn.execute("SELECT user_id FROM sessions WHERE token=?", (token,)).fetchone()
        if not row:
            return None
        user = conn.execute("SELECT * FROM users WHERE id=?", (row["user_id"],)).fetchone()
        return dict(user) if user else None
    finally:
        conn.close()


def delete_session(token: str):
    conn = get_db()
    try:
        conn.execute("DELETE FROM sessions WHERE token=?", (token,))
        conn.commit()
    finally:
        conn.close()


def create_api_key(user_id: str, name: str = "") -> dict:
    key = "sk-" + secrets.token_urlsafe(24)
    kid = uuid.uuid4().hex[:16]
    conn = get_db()
    try:
        conn.execute("INSERT INTO api_keys(id, user_id, key, name, created_at) VALUES (?,?,?,?,?)",
                     (kid, user_id, key, name, time.strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        row = conn.execute("SELECT * FROM api_keys WHERE id=?", (kid,)).fetchone()
        return dict(row)
    finally:
        conn.close()


def list_api_keys(user_id: str):
    conn = get_db()
    try:
        rows = conn.execute("SELECT id, name, created_at, last_used_at FROM api_keys WHERE user_id=?", (user_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def delete_api_key(kid: str, user_id: str):
    conn = get_db()
    try:
        conn.execute("DELETE FROM api_keys WHERE id=? AND user_id=?", (kid, user_id))
        conn.commit()
    finally:
        conn.close()


def get_user_by_api_key(key: str):
    if not key:
        return None
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM api_keys WHERE key=?", (key,)).fetchone()
        if not row:
            return None
        conn.execute("UPDATE api_keys SET last_used_at=? WHERE id=?", (time.strftime("%Y-%m-%d %H:%M:%S"), row["id"]))
        conn.commit()
        user = conn.execute("SELECT * FROM users WHERE id=?", (row["user_id"],)).fetchone()
        return dict(user) if user else None
    finally:
        conn.close()


def consume_quota(user_id: str, cost: int = 1) -> bool:
    """消耗额度；成功返回 True"""
    conn = get_db()
    try:
        row = conn.execute("SELECT quota FROM users WHERE id=?", (user_id,)).fetchone()
        if not row or row["quota"] < cost:
            return False
        conn.execute("UPDATE users SET quota=quota-? WHERE id=?", (cost, user_id))
        conn.execute("INSERT INTO usage_log(user_id, action, cost, ts) VALUES (?,?,?,?)",
                     (user_id, "call", cost, time.strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        return True
    finally:
        conn.close()


def add_quota(user_id: str, n: int):
    conn = get_db()
    try:
        conn.execute("UPDATE users SET quota=quota+?, total_quota=total_quota+? WHERE id=?", (n, n, user_id))
        conn.commit()
    finally:
        conn.close()


def get_usage(user_id: str, limit: int = 50):
    conn = get_db()
    try:
        rows = conn.execute("SELECT action, cost, ts FROM usage_log WHERE user_id=? ORDER BY id DESC LIMIT ?",
                            (user_id, limit)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_setting(key: str, default: str = "") -> str:
    conn = get_db()
    try:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default
    finally:
        conn.close()


def get_all_settings() -> dict:
    conn = get_db()
    try:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}
    finally:
        conn.close()


def set_setting(key: str, value: str):
    conn = get_db()
    try:
        conn.execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?,?)", (key, value))
        conn.commit()
    finally:
        conn.close()


def list_users():
    conn = get_db()
    try:
        rows = conn.execute("SELECT id, username, role, quota, total_quota, status, created_at FROM users ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def update_user(uid: str, **fields):
    conn = get_db()
    try:
        sets = ", ".join(f"{k}=?" for k in fields)
        conn.execute(f"UPDATE users SET {sets} WHERE id=?", (*fields.values(), uid))
        conn.commit()
    finally:
        conn.close()


def stats():
    conn = get_db()
    try:
        users = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        calls = conn.execute("SELECT COUNT(*) c FROM usage_log").fetchone()["c"]
        keys = conn.execute("SELECT COUNT(*) c FROM api_keys").fetchone()["c"]
        total_quota = conn.execute("SELECT COALESCE(SUM(total_quota),0) s FROM users").fetchone()["s"]
        used = conn.execute("SELECT COALESCE(SUM(cost),0) s FROM usage_log").fetchone()["s"]
        return {"users": users, "api_calls": calls, "api_keys": keys,
                "total_quota": total_quota, "used_quota": used}
    finally:
        conn.close()


def public_settings() -> dict:
    s = get_all_settings()
    return {
        "site_name": s.get("site_name", ""),
        "announcement": s.get("announcement", ""),
        "payment_enabled": s.get("payment_enabled", "0") == "1",
        "contact_url": s.get("contact_url", ""),
        "contact_text": s.get("contact_text", ""),
        "trial_quota": s.get("trial_quota", ""),
        "price_per_quota": s.get("price_per_quota", ""),
        "quota_package": s.get("quota_package", ""),
    }
