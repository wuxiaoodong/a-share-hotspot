#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""微信推送器：通过 PushPlus(推送加) 把日报全文推到【个人微信】。

用法:
    export PUSHPLUS_TOKEN=你的token
    python3 push_wechat.py <日报.md> [消息标题]

- 免费 200 条/天（每日日报只发 1 条，绰绰有余）
- 微信需先扫码关注「pushplus 推送加」服务号，手机号实名后取 token
- 接口: POST https://www.pushplus.plus/send  (json body)
- 纯标准库，零依赖，Linux/macOS/Windows 通用

前置: 用户需在 https://www.pushplus.plus 微信扫码登录 → 实名 → 个人中心复制 token
"""
import json
import os
import sys
import time
import urllib.request

API = "https://www.pushplus.plus/send"
MAX_BYTES = 80000  # 防御性上限（日报实际约7KB，远达不到）


def send_markdown(token, title, content, retries=2):
    """发送 markdown 到微信。成功返回0，失败返回1。"""
    body = json.dumps({
        "token": token,
        "title": title,
        "content": content,
        "template": "markdown",
    }, ensure_ascii=False).encode("utf-8")
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                API, data=body,
                headers={"Content-Type": "application/json"},
                method="POST")
            with urllib.request.urlopen(req, timeout=15) as resp:
                ret = json.loads(resp.read().decode("utf-8", "ignore"))
            if ret.get("code") == 200:
                print("PUSHPLUS OK:", ret.get("msg"))
                return 0
            last_err = f"code={ret.get('code')} msg={ret.get('msg')} data={ret.get('data')}"
        except Exception as e:  # noqa: BLE001
            last_err = repr(e)
        print(f"  (第{attempt + 1}次失败: {last_err}, 重试...)")
        time.sleep(3)
    print("PUSHPLUS FAIL:", last_err, file=sys.stderr)
    return 1


def main():
    if len(sys.argv) < 2:
        print("用法: python3 push_wechat.py <日报.md> [消息标题]", file=sys.stderr)
        return 2
    token = os.environ.get("PUSHPLUS_TOKEN", "").strip()
    if not token:
        print("缺少 PUSHPLUS_TOKEN 环境变量", file=sys.stderr)
        return 2
    path = sys.argv[1]
    title = sys.argv[2] if len(sys.argv) > 2 else "A股热点资金趋势日报"
    if not os.path.exists(path):
        print(f"找不到文件: {path}", file=sys.stderr)
        return 2
    content = open(path, encoding="utf-8").read()
    if len(content.encode("utf-8")) > MAX_BYTES:
        content = content[:MAX_BYTES // 3]  # 按字符截断，避免裁到多字节边界
        print("(内容超长已截断)")
    print(f"推送文件: {path} | 内容 {len(content.encode('utf-8'))} 字节")
    return send_markdown(token, title, content)


if __name__ == "__main__":
    sys.exit(main())
