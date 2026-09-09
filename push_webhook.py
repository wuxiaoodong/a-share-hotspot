# -*- coding: utf-8 -*-
"""
A股热点资金趋势日报 - 企业微信群机器人 Webhook 推送器(云端无人值守版)
读取 wecom_full.md, 按 <=3800字节/条 自动拆分(企微群机器人markdown上限4096字节),
逐条 POST 到群机器人 webhook。纯标准库, 无需 pip 依赖。

用法:
    python3 push_webhook.py <webhook_url>
    python3 push_webhook.py <配置文件>     # 文件内: WEBHOOK="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"
    python3 push_webhook.py                # 自动读取同目录 .webhook_key 文件(同上格式), 或环境变量 WECOM_WEBHOOK

退出码: 0=全部发送成功; 非0=失败(供 cron 记录)
"""
import http.client
import json
import os
import ssl
import sys
import time

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

MAX_BYTES = 3800          # 单条 content 字节上限(留余量, 官方4096)
WEBHOOK_ENDPOINT = "qyapi.weixin.qq.com"
TLS_CTX = ssl.create_default_context()


def resolve_webhook(argv):
    """从 命令行参数 / 配置文件 / 环境变量 解析 webhook url"""
    if len(argv) > 1:
        a = argv[1]
        if a.startswith("http"):
            return a
        with open(a, encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if ln.startswith("WEBHOOK="):
                    return ln.split("=", 1)[1].strip().strip('"').strip("'")
    # 同目录 .webhook_key
    keyf = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".webhook_key")
    if os.path.exists(keyf):
        with open(keyf, encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if ln.startswith("WEBHOOK="):
                    return ln.split("=", 1)[1].strip().strip('"').strip("'")
    env = os.environ.get("WECOM_WEBHOOK")
    if env:
        return env
    sys.exit("未找到 webhook。用法: push_webhook.py <webhook_url> 或配置文件/.webhook_key/环境变量 WECOM_WEBHOOK")


def split_messages(text, limit=MAX_BYTES):
    """按行累积拆分, 保证每条 <=limit 字节; 优先在空行/■/【/** 前断块"""
    lines = text.split("\n")
    msgs = []

    def flush(buf):
        if buf.strip():
            msgs.append(buf)
        return ""

    buf = ""
    for ln in lines:
        if not ln.strip():                      # 空行: 段间自然断点
            buf = flush(buf)
            continue
        if buf and (ln.startswith("■") or ln.startswith("【") or ln.startswith("**")) \
                and len(buf.encode("utf-8")) > 1200:
            buf = flush(buf)
        nxt = buf + "\n" + ln if buf else ln
        if len(nxt.encode("utf-8")) <= limit:
            buf = nxt
        else:
            buf = flush(buf)
            # 单行超长时折半切(罕见)
            if len(ln.encode("utf-8")) > limit:
                while ln:
                    cut = max(1, limit * 3 // 4)
                    while cut < len(ln) and len(ln[:cut].encode("utf-8")) < limit:
                        cut += 1
                    msgs.append(ln[:cut])
                    ln = ln[cut:]
            else:
                buf = ln
    buf = flush(buf)
    return msgs


def send_markdown(webhook, content, retries=3):
    from urllib.parse import urlsplit
    u = urlsplit(webhook)
    if u.scheme != "https" or "webhook" not in u.path:
        raise ValueError(f"webhook 格式不正确: {webhook}")
    payload = json.dumps({"msgtype": "markdown", "markdown": {"content": content}},
                         ensure_ascii=False).encode("utf-8")
    path = u.path + (("?" + u.query) if u.query else "")
    last = None
    for i in range(retries):
        try:
            conn = http.client.HTTPSConnection(u.netloc, timeout=15, context=TLS_CTX)
            conn.request("POST", path, body=payload, headers={
                "Content-Type": "application/json; charset=utf-8",
                "Content-Length": str(len(payload)),
            })
            resp = conn.getresponse()
            raw = resp.read().decode("utf-8", "ignore")
            conn.close()
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} {raw[:200]}")
            d = json.loads(raw)
            if d.get("errcode") == 0:
                return d
            raise RuntimeError(f"errcode={d.get('errcode')} errmsg={d.get('errmsg')}")
        except Exception as e:  # noqa
            last = e
            time.sleep(2.0 * (i + 1))
    raise RuntimeError(f"webhook 发送失败: {last}")


def main():
    webhook = resolve_webhook(sys.argv)
    src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wecom_full.md")
    if not os.path.exists(src):
        src = "wecom_full.md"
    if not os.path.exists(src):
        sys.exit(f"未找到 {src}, 请先运行 run_daily_lite.py 生成日报")
    text = open(src, encoding="utf-8").read()
    msgs = split_messages(text)
    if not msgs:
        sys.exit("wecom_full.md 内容为空, 无可推送消息")
    total_bytes = sum(len(m.encode("utf-8")) for m in msgs)
    print(f"共拆分 {len(msgs)} 条消息, 总 {total_bytes} 字节", flush=True)
    for i, m in enumerate(msgs, 1):
        try:
            d = send_markdown(webhook, m)
            print(f"[{i}/{len(msgs)}] 已发送 {len(m.encode('utf-8'))}B errcode={d.get('errcode')}", flush=True)
        except Exception as e:  # noqa
            print(f"[{i}/{len(msgs)}] 发送失败: {e}", flush=True)
            sys.exit(2)
        time.sleep(1.0)       # 群机器人限流: 20条/分钟, 留足余量
    print("==> 日报已全部推送到企业微信群 ✅", flush=True)


if __name__ == "__main__":
    main()
