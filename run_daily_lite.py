# -*- coding: utf-8 -*-
"""
A股热点资金趋势追踪工作台 - 精简版管线 v2
数据源: 东方财富 push2delay clist 实时接口(含今日/3日/5日/10日累计主力资金流)
        + push2his fflow/daykline 板块逐日资金/涨幅历史(被封时自动降级读本地缓存)
口径: f62=今日主力净额, f164/f174/f267=3/5/10日累计, f109=5日涨跌幅,
      f184=主力净占比, f104/f105=板块上涨/下跌家数; fflow行: [1]=主力净额(元) [12]=涨跌幅%
输出: reports/YYYY-MM-DD_日报.md + data/snapshots/YYYY-MM-DD.csv(含涨跌家数,积累发酵度)
      + data/fund_history/BKxxxx.json(逐日历史缓存) + wecom_summary.md + wecom_full.md
"""
import csv
import datetime
import glob
import http.client
import json
import os
import ssl
import sys
import time
import urllib.parse

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE = "push2delay.eastmoney.com"
HIS_BASE = "push2his.eastmoney.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REFERER = "https://data.eastmoney.com/bkzj/hy.html"
TLS_CTX = ssl.create_default_context()
_conn = None
_his_conn = None

BOARD_FIELDS = "f12,f14,f3,f6,f62,f104,f105,f128,f164,f174,f267,f109"
STOCK_FIELDS = "f12,f14,f2,f3,f6,f62,f164,f174,f109"

MIN_TURNOVER_YI = 5.0     # 板块最低成交额(亿) 过滤微型板块
TOP_SECTORS = 12          # 热点榜数量
STOCK_SECTORS = 8         # 深挖个股的板块数
STOCKS_PER_SECTOR = 5
HIST_DAYS = 10            # 历史趋势回看天数


# ---------------- HTTP ----------------
def _req(conn_holder, host, path, timeout):
    global _conn, _his_conn
    if conn_holder == "d":
        if _conn is None:
            _conn = http.client.HTTPSConnection(host, timeout=timeout, context=TLS_CTX)
        c = _conn
    else:
        if _his_conn is None:
            _his_conn = http.client.HTTPSConnection(host, timeout=timeout, context=TLS_CTX)
        c = _his_conn
    c.request("GET", path, headers={
        "User-Agent": UA, "Referer": REFERER,
        "Connection": "keep-alive", "Accept": "*/*",
    })
    resp = c.getresponse()
    body = resp.read()
    if resp.status != 200:
        raise RuntimeError(f"HTTP {resp.status}")
    return json.loads(body.decode("utf-8", "ignore"))


def _close(conn_holder):
    global _conn, _his_conn
    c = _conn if conn_holder == "d" else _his_conn
    try:
        if c:
            c.close()
    except Exception:
        pass
    if conn_holder == "d":
        _conn = None
    else:
        _his_conn = None


def http_get_json(params, retries=4):
    path = "/api/qt/clist/get?" + urllib.parse.urlencode(params)
    last = None
    for i in range(retries):
        try:
            return _req("d", BASE, path, 15)
        except Exception as e:  # noqa
            last = e
            _close("d")
            time.sleep(1.0 * (i + 1))
    raise RuntimeError(f"http fail: {last}")


def http_get_json_his(params, retries=1):
    """push2his 历史接口: 1次重试, 快速失败(被封时靠缓存降级)"""
    path = "/api/qt/stock/fflow/daykline/get?" + urllib.parse.urlencode(params)
    last = None
    for i in range(retries):
        try:
            return _req("h", HIS_BASE, path, 8)
        except Exception as e:  # noqa
            last = e
            _close("h")
            time.sleep(1.0)
    raise RuntimeError(f"his fail: {last}")


# ---------------- 数据抓取 ----------------
def fetch_boards(fs):
    out, pn = [], 1
    while True:
        d = http_get_json({
            "pn": pn, "pz": 100, "po": 1, "np": 1, "fltt": 2, "invt": 2,
            "fid": "f62", "fs": fs, "fields": BOARD_FIELDS,
        })
        data = (d.get("data") or {})
        diff = data.get("diff") or []
        if not diff:
            break
        out.extend(diff)
        if len(out) >= int(data.get("total") or 0) or pn > 12:
            break
        pn += 1
        time.sleep(0.6)
    return out


def fetch_sector_stocks(bk_code):
    d = http_get_json({
        "pn": 1, "pz": 50, "po": 1, "np": 1, "fltt": 2, "invt": 2,
        "fid": "f62", "fs": f"b:{bk_code}", "fields": STOCK_FIELDS,
    })
    return (d.get("data") or {}).get("diff") or []


def parse_fflow_klines(d, asof):
    """fflow daykline -> [(date, main_yi, pct)], 剔除今日(以实时数据为准)"""
    rows = ((d.get("data") or {}).get("klines")) or []
    out = []
    for r in rows:
        p = r.split(",")
        if len(p) < 13:
            continue
        try:
            date = p[0]
            if date == asof:
                continue
            out.append((date, float(p[1]) / 1e8, float(p[12])))
        except (ValueError, IndexError):
            continue
    return out


def load_fund_history(bk, asof):
    """板块逐日历史: 优先直连 push2his(成功则更新缓存), 失败读本地缓存"""
    cache_dir = os.path.join("data", "fund_history")
    os.makedirs(cache_dir, exist_ok=True)
    cache = os.path.join(cache_dir, f"{bk}.json")
    params = {
        "secid": f"90.{bk}", "lmt": HIST_DAYS + 2, "klt": 101,
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
    }
    src = "无"
    try:
        d = http_get_json_his(params)
        with open(cache, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
        hist = parse_fflow_klines(d, asof)
        src = "直连"
    except Exception:
        if os.path.exists(cache):
            try:
                with open(cache, encoding="utf-8") as f:
                    d = json.load(f)
                hist = parse_fflow_klines(d, asof)
                src = "缓存"
            except Exception:
                hist = []
        else:
            hist = []
    return hist[-HIST_DAYS:], src


def load_breadth_history(asof):
    """读取历史快照 -> {sector_id: [(date, up, down, pct, main_yi), ...]按日期升序}"""
    hist = {}
    for fp in sorted(glob.glob(os.path.join("data", "snapshots", "*.csv"))):
        try:
            with open(fp, encoding="utf-8-sig", newline="") as f:
                for r in csv.DictReader(f):
                    if r.get("date") == asof or not r.get("sector_id"):
                        continue
                    hist.setdefault(r["sector_id"], []).append((
                        r["date"], int(float(r.get("up_cnt") or 0)),
                        int(float(r.get("down_cnt") or 0)),
                        float(r.get("pct_chg") or 0), float(r.get("main_net_yi") or 0),
                    ))
        except Exception:
            continue
    for k in hist:
        hist[k].sort()
    return hist


# ---------------- 指标 ----------------
def pct_rank(vals, v):
    if v is None or not vals:
        return 0.0
    s = sorted(x for x in vals if x is not None)
    if not s:
        return 0.0
    lo, hi = 0, len(s)
    while lo < hi:
        mid = (lo + hi) // 2
        if s[mid] < v:
            lo = mid + 1
        else:
            hi = mid
    return lo / len(s) * 100.0


def num(x):
    try:
        f = float(x)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def build_board(row, btype):
    f62 = num(row.get("f62"))
    f6 = num(row.get("f6"))
    f164 = num(row.get("f164"))
    f174 = num(row.get("f174"))
    f267 = num(row.get("f267"))
    b = {
        "id": row.get("f12"), "name": row.get("f14"), "type": btype,
        "pct": num(row.get("f3")),
        "turnover_yi": (f6 or 0) / 1e8,
        "main_yi": (f62 or 0) / 1e8,
        "up": num(row.get("f104")) or 0, "down": num(row.get("f105")) or 0,
        "lead": row.get("f128") or "-",
        "r5": num(row.get("f109")),
    }
    s1 = f62 if f62 is not None else 0
    s2 = (f164 - f62) if (f164 is not None and f62 is not None) else None
    s3 = (f174 - f164) if (f174 is not None and f164 is not None) else None
    s4 = (f267 - f174) if (f267 is not None and f174 is not None) else None
    b["segs"] = [s1, s2, s3, s4]
    b["cum3_yi"] = (f164 or 0) / 1e8
    b["cum5_yi"] = (f174 or 0) / 1e8
    b["cum10_yi"] = (f267 or 0) / 1e8
    b["ratio"] = (f62 / f6 * 100) if (f62 and f6) else 0.0
    tot = b["up"] + b["down"]
    b["diffusion"] = (b["up"] / tot) if tot > 0 else 0.5
    streak = 0
    for s in b["segs"]:
        if s is not None and s > 0:
            streak += 1
        else:
            break
    b["streak"] = streak
    return b


def lifecycle(b):
    s1, s2, s3, s4 = b["segs"]
    hot = b["pct"] or 0
    r5 = b["r5"] or 0
    if hot > 3 and b["ratio"] > 5 and r5 > 8:
        return "高潮"
    if r5 > 3 and s1 is not None and s1 < 0 and b["cum5_yi"] > 0:
        return "撤退"
    if b["streak"] >= 3 and hot > 0:
        return "强化"
    if b["streak"] >= 2 and r5 > 3:
        return "强势延续"
    if s1 is not None and s1 > 0 and s2 is not None and s2 > 0 and r5 < 3:
        return "形成"
    if s1 is not None and s1 > 0 and (s2 is None or s2 <= 0) and r5 < 0:
        return "试探"
    return "观察"


def series_stats(hist, b):
    """hist: [(date, main_yi, pct)] 历史日(不含今日); 追加今日实时值后统计"""
    series = list(hist) + [(b.get("_asof"), b["main_yi"], b["pct"] or 0)]
    # 连续净流入天数(从今日往前)
    fund_streak = 0
    for _, m, _p in reversed(series):
        if m > 0:
            fund_streak += 1
        else:
            break
    # 连续上涨天数
    up_streak = 0
    for _, _m, p in reversed(series):
        if p > 0:
            up_streak += 1
        else:
            break
    net_sum = sum(m for _, m, _ in series)
    return series, fund_streak, up_streak, net_sum


def ferment_info(b, breadth_hist):
    """板内上涨家数逐日变化(发酵度): 基于历史快照"""
    rows = breadth_hist.get(b["id"]) or []
    if not rows:
        return 0, None  # 连增天数, 明细
    ups = [(d, u) for d, u, _dn, _p, _m in rows] + [(b.get("_asof"), b["up"])]
    # 从最近一天往前数"逐日增加"
    inc = 0
    for i in range(len(ups) - 1, 0, -1):
        if ups[i][1] > ups[i - 1][1]:
            inc += 1
        else:
            break
    detail = "→".join(f"{u}" for _d, u in ups[-5:])
    return inc, detail


def entry_advice(b):
    """进场参考: 结合生命周期/资金连续性/板内发酵"""
    life = b["life"]
    fs = b.get("fund_streak_d") or b["streak"]
    diff = b["diffusion"]
    if life == "高潮":
        return "情绪高潮·谨慎追高"
    if life == "撤退":
        return "资金撤离·回避"
    if life in ("试探", "形成") and fs >= 2 and diff > 0.55:
        return "★左侧关注·可分批进场"
    if (b.get("ferment_inc") or 0) >= 2:
        return "★板内发酵·上涨家数连增·重点关注"
    if life in ("强化", "强势延续") and fs >= 3:
        return "顺势持有·防高位分歧"
    if life in ("强化", "强势延续"):
        return "持有观察·趋势延续中"
    return "观望"


def probe_latest_trade_date(asof):
    """探测上证指数(1.000001)最新行情日期 -> 'yyyymmdd'; 网络异常/字段缺失返回None(放行)
    f86 为 Unix 秒时间戳(UTC), 换算为北京日期后再比较"""
    try:
        path = "/api/qt/stock/get?" + urllib.parse.urlencode({
            "secid": "1.000001", "invt": 2, "fltt": 2,
            "fields": "f86", "ut": "fa5fd1943c7b386f172d6893dbfba10b"})
        d = _req("d", BASE, path, 8)
        ts = (d.get("data") or {}).get("f86")
        if ts is None:
            return None
        t = int(ts)
        if t <= 0:
            return None
        bj = datetime.datetime.fromtimestamp(t, datetime.timezone.utc) \
            + datetime.timedelta(hours=8)
        return bj.strftime("%Y%m%d")
    except Exception as e:  # noqa
        print(f"    (交易日探测异常: {e}, 按交易日继续)")
    return None


# ---------------- 主流程 ----------------
def main():
    t0 = time.time()
    label = sys.argv[1] if len(sys.argv) > 1 else ""
    # 统一用北京时间(UTC+8)判定日期: 兼容海外执行机(如GitHub Actions默认UTC)不因时区/延迟跨日错判
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)
    now = now.replace(tzinfo=None)
    asof = now.date().isoformat()

    # ---- 交易日探测: 上证指数最新行情日期 != 今天 -> 休市(法定节假日), 静默退出 ----
    print("==> 0/5 交易日探测 ...")
    yd = probe_latest_trade_date(asof)
    if yd is not None and yd != asof.replace("-", ""):
        print(f"    上证最新行情日期 {yd} != 今天({asof}) -> 今日休市/非交易日, 退出不生成日报")
        return
    print("    今日为交易日 ✓")

    print("==> 1/5 拉取行业/概念板块资金数据 ...")
    ind_rows = fetch_boards("m:90+t:2")
    time.sleep(1.0)
    con_rows = fetch_boards("m:90+t:3")
    print(f"    行业 {len(ind_rows)} 个, 概念 {len(con_rows)} 个")

    pool = [build_board(r, "行业") for r in ind_rows] + \
           [build_board(r, "概念") for r in con_rows]
    for b in pool:
        b["_asof"] = asof
    pool = [b for b in pool if b["turnover_yi"] >= MIN_TURNOVER_YI]
    print(f"    过滤低成交板块后 {len(pool)} 个")

    print("==> 2/5 计算 HotScore / 生命周期 / 潜在新热点 ...")
    v_main = [b["main_yi"] for b in pool]
    v_ratio = [b["ratio"] for b in pool]
    v_cum5 = [b["cum5_yi"] for b in pool]
    v_streak = [float(b["streak"]) for b in pool]
    v_r5 = [b["r5"] or 0.0 for b in pool]
    v_pct = [b["pct"] or 0.0 for b in pool]
    v_diff = [b["diffusion"] for b in pool]
    v_turn = [b["turnover_yi"] for b in pool]

    for b in pool:
        sc = (pct_rank(v_main, b["main_yi"]) * 0.25
              + pct_rank(v_ratio, b["ratio"]) * 0.15
              + pct_rank(v_cum5, b["cum5_yi"]) * 0.10
              + pct_rank(v_streak, float(b["streak"])) * 0.05
              + pct_rank(v_r5, b["r5"] or 0.0) * 0.20
              + pct_rank(v_pct, b["pct"] or 0.0) * 0.10
              + pct_rank(v_diff, b["diffusion"]) * 0.10
              + pct_rank(v_turn, b["turnover_yi"]) * 0.05)
        b["hotscore"] = round(sc, 1)
        b["life"] = lifecycle(b)
        b["new_hot"] = (b["main_yi"] > 0
                        and pct_rank(v_ratio, b["ratio"]) > 80
                        and (b["r5"] if b["r5"] is not None else 0) < 2
                        and b["diffusion"] > 0.55)

    pool.sort(key=lambda x: -x["hotscore"])
    top = pool[:TOP_SECTORS]
    new_hots = [b for b in pool if b["new_hot"] and b["main_yi"] > 0.5]
    new_hots.sort(key=lambda x: -x["main_yi"])
    new_hots = new_hots[:8]

    # ---- 多日趋势: 热点榜 + 潜在新热点 ----
    print("==> 3/5 拉取板块逐日资金/涨幅历史(近10日) ...")
    trend_boards, seen = [], set()
    for b in top + new_hots:
        if b["id"] not in seen:
            seen.add(b["id"])
            trend_boards.append(b)
    breadth_hist = load_breadth_history(asof)
    n_direct = 0
    for b in trend_boards:
        hist, src = load_fund_history(b["id"], asof)
        b["hist_src"] = src
        if src == "直连":
            n_direct += 1
            time.sleep(0.5)
        series, fs, us, net = series_stats(hist, b)
        b["series"] = series
        b["fund_streak_d"] = fs
        b["up_streak_d"] = us
        b["net_sum_yi"] = net
        inc, detail = ferment_info(b, breadth_hist)
        b["ferment_inc"] = inc
        b["ferment_detail"] = detail
        b["advice"] = entry_advice(b)
    print(f"    历史来源: 直连{n_direct}个/缓存{sum(1 for b in trend_boards if b['hist_src']=='缓存')}个/无{sum(1 for b in trend_boards if b['hist_src']=='无')}个")

    # ---- 个股深挖 ----
    print("==> 4/5 重点板块个股筛选 ...")
    stock_tables = []
    for b in top[:STOCK_SECTORS]:
        try:
            rows = fetch_sector_stocks(b["id"])
        except Exception as e:  # noqa
            print(f"    warn {b['name']}: {e}")
            continue
        stocks = []
        for r in rows:
            f62 = num(r.get("f62"))
            if f62 is None or f62 <= 0:
                continue
            stocks.append({
                "code": r.get("f12"), "name": r.get("f14"),
                "pct": num(r.get("f3")),
                "main_yi": f62 / 1e8,
                "cum3_yi": (num(r.get("f164")) or 0) / 1e8,
                "cum5_yi": (num(r.get("f174")) or 0) / 1e8,
                "r5": num(r.get("f109")),
            })
        v1 = [s["main_yi"] for s in stocks]; v2 = [s["cum3_yi"] for s in stocks]
        v3 = [s["cum5_yi"] for s in stocks]; v4 = [s["pct"] or 0 for s in stocks]
        v5 = [s["r5"] or 0 for s in stocks]
        for s in stocks:
            s["score"] = round(pct_rank(v1, s["main_yi"]) * 0.30
                               + pct_rank(v2, s["cum3_yi"]) * 0.30
                               + pct_rank(v3, s["cum5_yi"]) * 0.20
                               + pct_rank(v4, s["pct"] or 0) * 0.10
                               + pct_rank(v5, s["r5"] or 0) * 0.10, 1)
        stocks.sort(key=lambda x: -x["score"])
        stock_tables.append((b, stocks[:STOCKS_PER_SECTOR]))
        print(f"    {b['name']}: {len(stocks)} 只候选")
        time.sleep(0.8)

    # ---- 报告 ----
    print("==> 5/5 生成日报 ...")
    ind_pool = [b for b in pool if b["type"] == "行业"]
    up_boards = sum(1 for b in ind_pool if (b["pct"] or 0) > 0)
    down_boards = sum(1 for b in ind_pool if (b["pct"] or 0) < 0)
    ind_in_top = sorted(ind_pool, key=lambda x: -x["main_yi"])
    has_hist = any(b.get("series") and len(b["series"]) > 1 for b in trend_boards)

    def seg_mark(b):
        return "".join("█" if (s is not None and s > 0) else "·" for s in b["segs"])

    def day_marks(series):
        """近10日资金脉络: █净流入 ·净流出, [今日]"""
        tail = series[-HIST_DAYS:]
        s = "".join("█" if m > 0 else "·" for _d, m, _p in tail)
        return s[:-1] + "[" + s[-1] + "]" if s else "-"

    def fmt_series(series, key, unit=""):
        tail = series[-6:]
        return " ".join(f"{v:+.1f}{unit}" for _d, _m, v in tail) if key == "pct" \
            else " ".join(f"{v:+.1f}{unit}" for _d, v, _p in tail)

    # ================= Markdown 完整版 =================
    L = []
    L.append(f"# A股热点资金趋势日报 · {asof}")
    if label:
        L.append(f"\n> 模式: {label}  |  生成时间: {now.strftime('%Y-%m-%d %H:%M')}")
    L.append("\n## 一、市场资金总览\n")
    L.append(f"- 行业板块上涨/下跌: **{up_boards} / {down_boards}** (共{len(ind_pool)}个)")
    L.append("- 今日主力净流入前三行业: " + "、".join(
        f"{b['name']}({b['main_yi']:+.1f}亿)" for b in ind_in_top[:3]))
    L.append("- 今日主力净流出前三行业: " + "、".join(
        f"{b['name']}({b['main_yi']:+.1f}亿)" for b in sorted(ind_in_top, key=lambda x: x['main_yi'])[:3]))
    con_sort = sorted([b for b in pool if b["type"] == "概念"], key=lambda x: -x["main_yi"])
    L.append("- 概念板块主力净流入前三: " + "、".join(
        f"{b['name']}({b['main_yi']:+.1f}亿)" for b in con_sort[:3]))

    L.append(f"""
## 二、热点板块趋势与进场参考 (HotScore Top {len(top)})

资金脉络(近{HIST_DAYS}日, █=净流入 ·=净流出, [今日]); 进场参考综合: 生命周期+资金连续性+板内发酵
""")
    for i, b in enumerate(top, 1):
        r5 = f"{b['r5']:+.1f}%" if b["r5"] is not None else "-"
        L.append(f"### {i}. {b['name']} ({b['type']}·{b['life']}·HotScore {b['hotscore']})")
        L.append(f"- 今日: 涨{b['pct']:+.2f}% | 主力{b['main_yi']:+.1f}亿(净占比{b['ratio']:.1f}%) | 5日{r5} | 领涨: {b['lead']}")
        if b.get("series") and len(b["series"]) > 1:
            L.append(f"- 近{HIST_DAYS}日资金脉络: {day_marks(b['series'])} | 连续净流入 **{b['fund_streak_d']}天** | 连涨 **{b['up_streak_d']}天** | {HIST_DAYS}日累计 {b['net_sum_yi']:+.1f}亿")
            L.append(f"- 近6日涨幅: {fmt_series(b['series'], 'pct', '%')} | 近6日主力(亿): {fmt_series(b['series'], 'main')}")
        else:
            L.append(f"- 资金分段: {seg_mark(b)} (历史接口受限, 仅展示分段)")
        tot = b["up"] + b["down"]
        fer = f"发酵度: 上涨家数{b['ferment_detail']}(连增{b['ferment_inc']}天) 🔥" if b.get("ferment_detail") else "发酵度: 数据自今日积累"
        L.append(f"- 板内上涨: **{int(b['up'])}/{int(tot)}家** | {fer}")
        L.append(f"- **进场参考: {b['advice']}**")
        L.append("")

    L.append("""
## 三、🔴 潜在新热点预警 (资金新进·涨幅尚低)

判定: 今日主力净占比居前(前20%) + 板内上涨家数>55% + 5日涨幅<2%(行情尚未发酵)
""")
    if new_hots:
        for b in new_hots:
            r5 = f"{b['r5']:+.1f}%" if b["r5"] is not None else "-"
            tot = b["up"] + b["down"]
            line = (f"- **{b['name']}**({b['type']}) 主力{b['main_yi']:+.1f}亿 净占比{b['ratio']:.1f}% "
                    f"今日{b['pct']:+.2f}% 5日{r5} 板内涨{int(b['up'])}/{int(tot)}家")
            if b.get("series") and len(b["series"]) > 1:
                line += f" | 连续净流入{b['fund_streak_d']}天 {day_marks(b['series'])}"
            L.append(line)
    else:
        L.append("今日无满足条件的潜在新热点。")

    L.append("\n## 四、重点板块强势股 (机会分 = 板内资金30+3日30+5日20+动量20)")
    for b, stocks in stock_tables:
        L.append(f"\n**{b['name']}** ({b['type']}·{b['life']}·HotScore {b['hotscore']}·参考: {b['advice']})")
        if not stocks:
            L.append("- 今日无主力净流入个股")
            continue
        L.append("| 代码 | 名称 | 今日涨幅 | 今日主力净额 | 3日累计 | 5日累计 | 5日涨幅 | 机会分 |")
        L.append("|---|---|---|---|---|---|---|---|")
        for s in stocks:
            r5 = f"{s['r5']:+.1f}%" if s["r5"] is not None else "-"
            L.append(f"| {s['code']} | {s['name']} | {s['pct'] or 0:+.2f}% | {s['main_yi']:+.2f}亿 | "
                     f"{s['cum3_yi']:+.2f}亿 | {s['cum5_yi']:+.2f}亿 | {r5} | **{s['score']}** |")

    L.append(f"""
## 五、口径与风险说明

- 数据源: 东方财富公开行情接口(延迟15分钟), 主力=超大单+大单
- 逐日历史: push2his 资金流日K(接口自带每日涨幅), 被限流时自动降级为累计分段
- 板内发酵度(上涨家数逐日变化)由每日快照积累, 快照保存于 data/snapshots/
- 本报告为程序化筛选结果, 不构成投资建议; 高潮期板块注意波动风险
- 生成耗时 {time.time()-t0:.0f}s
""")
    report = "\n".join(L)
    os.makedirs("reports", exist_ok=True)
    rp = os.path.join("reports", f"{asof}_日报.md")
    with open(rp, "w", encoding="utf-8") as f:
        f.write(report)

    # ---- 快照(含涨跌家数, 积累发酵度) ----
    snap_dir = os.path.join("data", "snapshots")
    os.makedirs(snap_dir, exist_ok=True)
    snap = os.path.join(snap_dir, f"{asof}.csv")
    with open(snap, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "sector_id", "name", "type", "main_net_yi",
                    "turnover_yi", "pct_chg", "main_ratio", "hotscore", "life",
                    "up_cnt", "down_cnt", "fund_streak"])
        for b in pool:
            w.writerow([asof, b["id"], b["name"], b["type"], round(b["main_yi"], 2),
                        round(b["turnover_yi"], 1), b["pct"], round(b["ratio"], 2),
                        b["hotscore"], b["life"], b["up"], b["down"],
                        b.get("fund_streak_d", b["streak"])])

    # ================= WeCom 摘要 =================
    wl = [f"【A股热点资金趋势日报 {asof}】{label}", ""]
    wl.append(f"■ 行业板块涨跌 {up_boards}/{down_boards}")
    wl.append("■ 今日热点榜(HotScore):")
    for i, b in enumerate(top[:6], 1):
        r5 = f"5日{b['r5']:+.1f}%" if b["r5"] is not None else ""
        fs = f" 连续净流入{b['fund_streak_d']}天" if b.get("fund_streak_d") else ""
        wl.append(f"{i}. {b['name']}[{b['life']}] 涨{b['pct']:+.2f}% 主力{b['main_yi']:+.1f}亿{r5}{fs} 分{b['hotscore']}")
    if new_hots:
        wl.append("■ 🔴潜在新热点(资金新进·未发酵):")
        for b in new_hots[:4]:
            wl.append(f"· {b['name']} 主力{b['main_yi']:+.1f}亿 净占比{b['ratio']:.1f}% 涨{b['pct']:+.2f}%")
    wl.append("")
    wl.append("完整趋势与个股池见下一条消息。")
    with open("wecom_summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(wl))

    # ================= WeCom/微信清爽版 =================
    # 设计目标：清眉目秀易读，每板块独立段+个股直接跟随，关键数字加粗
    # 推送通道：PushPlus 单条可达30KB，TOP5板块+每板块5只个股+5新热点 ≈ 2200字，宽松
    W = []
    W.append(f"# 📊 A股热点资金日报 · {asof}")
    W.append("")
    # 顶部摘要：今日最值得关注什么
    top1 = top[0]
    if len(top) >= 2:
        main_line = f"{top[0]['name']}/{top[1]['name']}持续走强"
    else:
        main_line = f"{top1['name']}走强"
    new_str = "、".join(b["name"] for b in new_hots[:3]) if new_hots else "暂无"
    W.append(f"**今日主线**：{main_line}（{top1['name']} {top1['hotscore']:.1f}分·{top1['life']}）；新资金悄然流入**{new_str}**等板块。")
    W.append("")
    W.append("---")
    W.append("")
    W.append(f"## 🥇 今日 TOP {min(5,len(top))} 热点板块")
    W.append("")
    # 用板块 bk 作为 key，索引到对应的个股列表
    stock_map = {b_b.get('bk') or b_b.get('id'): stocks for b_b, stocks in stock_tables}
    for i, b in enumerate(top[:5], 1):
        r5 = f"{b['r5']:+.1f}%" if b["r5"] is not None else "-"
        emoji = "⚠️" if "高潮" in b['life'] or "撤退" in b['life'] else "💡"
        W.append(f"### {['①','②','③','④','⑤','⑥','⑦','⑧','⑨','⑩'][i-1]} {b['name']} · {b['hotscore']:.1f}分 · {b['life']}")
        W.append(f"- 今日 涨 **{b['pct']:+.2f}%** ｜ 主力 **{b['main_yi']:+.1f}亿**")
        streak_parts = []
        if b.get("fund_streak_d"):
            streak_parts.append(f"主力连续净流入 **{b['fund_streak_d']}天**")
        if b.get("up_streak_d"):
            streak_parts.append(f"连涨 **{b['up_streak_d']}天**")
        streak_str = " ｜ ".join(streak_parts) if streak_parts else ""
        W.append(f"- 5日 涨 **{r5}**{(' ｜ ' + streak_str) if streak_str else ''}")
        if b.get("series") and len(b["series"]) > 1:
            ms = fmt_series(b['series'], 'main')
            W.append(f"- 近6日主力(亿)：{ms}")
        tot = b["up"] + b["down"]
        W.append(f"- 板内 **{int(b['up'])} / {int(tot)}** 家上涨")
        W.append(f"- {emoji} **{b['advice']}**")
        # ====== 核心个股直接跟在板块下面 ======
        b_stocks = stock_map.get(b.get('bk') or b.get('id'), [])
        if b_stocks:
            W.append("")
            W.append(f"  **🎯 核心个股**：")
            for s in b_stocks[:5]:
                r5s = f" 5日{s['r5']:+.1f}%" if s.get("r5") is not None else ""
                W.append(f"  · {s['name']}({s['code']}) 涨 **{s['pct'] or 0:+.2f}%** 主力 **+{s['main_yi']:.2f}亿**{r5s}")
        W.append("")
    W.append("---")
    W.append("")
    W.append("## 🌱 新热点预警（资金新进 · 未充分发酵）")
    W.append("")
    if new_hots:
        for b in new_hots[:5]:
            tot = b["up"] + b["down"]
            W.append(f"- **{b['name']}** ｜ 主力 **+{b['main_yi']:.1f}亿** ｜ 涨 {b['pct']:+.2f}% ｜ 板内 {int(b['up'])}/{int(tot)} 家上涨")
    else:
        W.append("今日无满足条件的潜在新热点。")
    W.append("")
    W.append("---")
    W.append("")
    W.append("_数据源：东方财富 ｜ 评分=资金40%+趋势30%+扩散15%+连续性15%_")
    W.append("_不构成投资建议_")
    wtext = "\n".join(W)
    with open("wecom_full.md", "w", encoding="utf-8") as f:
        f.write(wtext)

    print(f"    日报已生成: {rp}")
    print(f"    快照已保存: {snap}")
    print(f"    企微全文 {len(wtext.encode('utf-8'))} 字节, 摘要 {len(open('wecom_summary.md', encoding='utf-8').read().encode('utf-8'))} 字节")
    print("    TOP5: " + " | ".join(
        f"{b['name']}({b['hotscore']}/{b['advice'][:4]})" for b in top[:5]))
    if new_hots:
        print("    潜在新热点: " + " | ".join(b["name"] for b in new_hots[:5]))


if __name__ == "__main__":
    main()
