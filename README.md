# A股热点资金趋势日报 · GitHub Actions 版

每个交易日 16:00（北京时间）自动抓取东方财富板块资金数据，生成热点资金趋势日报，
通过 **PushPlus 推送到你的个人微信**。**完全免费、无需服务器、无需电脑开机、无需企微机器人。**

## 工作原理

```
GitHub Actions 免费机(ubuntu)  ── 北京时间16:00定时触发 ──┐
    │  1. run_daily_lite.py   抓数据→算HotScore→生成日报    │  data/snapshots 快照
    │  2. 休市探测: 上证行情日期≠今天则静默跳过              │  自动 commit 回仓库,
    │  3. push_wechat.py      全文markdown推送到个人微信     │  跨天积累发酵度数据
    └──────────────────────────────────────────────────────┘
```

- 定时：`0 8 * * 1-5`（UTC）= 周一至五 16:00 北京。免费机启动可能延迟数分钟至半小时，属正常。
- 节假日：脚本探测上证指数最新行情日期，非交易日自动跳过，不产生误报。
- 数据积累：板内上涨家数每日快照 + 板块资金历史缓存放 `data/`，由工作流每日自动 commit 回仓库。
- 可选：仓库配置 `WECOM_WEBHOOK` 密钥后，可同时向企业微信群机器人推送一份（不配置则只发个人微信）。

## 免费额度

- 仓库设为 **private**：GitHub Actions 每月免费 2000 分钟，本任务约 22 天/月 × 2~4 分钟 ≈ 90 分钟，绰绰有余。
- 设为 **public**：完全无限。
- PushPlus 免费 200 条/天（每日仅发 1 条）。

## 首次配置（PushPlus 微信通道，约 3 分钟）

1. 手机微信扫一扫打开 **https://www.pushplus.plus** → 微信扫码登录
2. 按提示完成**手机号实名**（国内合规要求，很快）
3. 关注公众号「**pushplus 推送加**」（收消息必需）
4. 登录后「个人中心」→ 复制你的 **token**
5. 把 token 配到仓库 Secrets（见下）

## 首次验证

1. 仓库 Settings → Secrets and variables → Actions → New repository secret
   名称：`PUSHPLUS_TOKEN`，值：第 4 步复制的 token
2. 仓库 Actions 页 → 左侧 "A股热点资金日报" → **Run workflow** → 手动跑一次
3. 若手动跑成功且**微信收到日报**，即上线完成；此后每日 16:00 自动运行。

## 目录说明

| 文件 | 作用 |
|---|---|
| `run_daily_lite.py` | 主分析管线（纯标准库，零依赖） |
| `push_wechat.py` | PushPlus 个人微信推送器（主通道） |
| `push_webhook.py` | 企微群机器人推送器（可选通道） |
| `.github/workflows/daily.yml` | 定时 + 推送 + 数据回写编排 |
| `data/fund_history/` | 板块逐日资金/涨幅历史缓存 |
| `data/snapshots/` | 每日板内上涨家数快照（发酵度积累源） |
| `reports/` | 每日完整日报存档 |
