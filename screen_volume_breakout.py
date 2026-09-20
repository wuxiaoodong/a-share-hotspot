# -*- coding: utf-8 -*-
"""
放量突破选股器
规则(用户定义, 2026-09-20 多次更正版):
  1) 基准期  : 最近20个交易日(T-21 ~ T-2, 剔除当日T与前1日T-1, 避免放量日污染基准)
               这20个交易日【每一天的】成交额都 < 1.0 亿元  (逐日判定, 非20日均)
  2) 价波约束: 基准期20日平均收盘价 = t
               且这20天里【每一天】的最高价 < 1.08*t 且 最低价 > 0.92*t  (±8%窄幅平台)
  3) 放  量  : T日成交额 / 基准均额 >= 3.0 倍
               且 T-1日成交额 / 基准均额 >= 3.0 倍  (连续两日放量)
  4) 上  涨  : T日涨幅 >= 5%

数据源: 新浪 money.finance.sina.com.cn (主) -> 腾讯 web.ifzq.gtimg.cn (备)
        新浪 volume 单位=股, 成交额 = volume x 收盘价 (与东财真实成交额吻合到 ~0.1%)
        腾讯 volume 单位=手, 成交额 = 手 x 100 x 收盘价
        (东财 push2his / 腾讯都对高频批量断连限流, 故: 收敛并发 + 本地缓存 + 断点续跑)
缓存:   data/kline_cache/{code}.json (当日有效, 重跑秒出)
输出:   screen_result_A/B/C.csv (三档口径) + 控制台表格
"""
import csv
import datetime
import http.client
import json
import os
import random
import ssl
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
TLS = ssl.create_default_context()

# ---------------- 可调参数 ----------------
QUIET_AMT_YI = 1.0   # 基准期逐日成交额上限(亿): 20个交易日每一天都要 < 此值
PRE_FILTER_YI = 3.0  # 初筛: 最新单日成交额 >= 此值的直接排除(必非安静股), 大幅减少kline请求
VOL_MULT    = 3.0    # 放量倍数下限
PCT_UP      = 5.0    # T日涨幅下限(%)
BAND_HI     = 1.08   # 基准期每日最高价 < BAND_HI * t
BAND_LO     = 0.92   # 基准期每日最低价 > BAND_LO * t
BOTH_DAYS   = True   # True=T日与T-1日均需放量; False=仅T日
MAX_WORKERS = 3      # 低频慢跑: 网关对高频批量会断连, 靠多轮累积缓存
KLINE_N     = 32     # 拉取日K根数
EXCLUDE_ST  = True
EXCLUDE_BJ  = True
CACHE_DIR   = os.path.join("data", "kline_cache")
CACHE_VER   = "v2"   # 缓存版本: v2起存6字段(date,close,vol,amt,high,low)
USE_CACHE   = True
_gate = threading.Semaphore(MAX_WORKERS)
_lock = threading.Lock()
_last_call = [0.0]


def _get(host, path, timeout=12, retries=3, hdr=None, raw=False):
    last = None
    for i in range(retries):
        try:
            c = http.client.HTTPSConnection(host, timeout=timeout, context=TLS)
            c.request("GET", path, headers=hdr or {"User-Agent": UA})
            r = c.getresponse()
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            body = r.read().decode("utf-8", "ignore")
            return body if raw else json.loads(body)
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(str(last))


def tx_symbol(code):
    if code.startswith(("60", "68", "9", "5")):
        return "sh" + code
    if code.startswith(("8", "4")):
        return "bj" + code
    return "sz" + code


def _throttle():
    """全局节流: 相邻请求之间至少间隔 250~400ms (约 3 QPS, 防封优先)"""
    with _lock:
        now = time.time()
        wait = _last_call[0] + random.uniform(0.25, 0.40) - now
        if wait > 0:
            time.sleep(wait)
        _last_call[0] = time.time()


def fetch_universe():
    """东财 clist 拉全市场A股代码+名称+最新单日成交额(push2delay 网关, 未限流)"""
    fs = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
    out, pn = [], 1
    while True:
        d = _get("push2delay.eastmoney.com", "/api/qt/clist/get?" + urllib.parse.urlencode({
            "pn": pn, "pz": 100, "po": 1, "np": 1, "fltt": 2, "invt": 2,
            "fid": "f12", "fs": fs, "fields": "f12,f14,f6",
        }), hdr={"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"})
        data = d.get("data") or {}
        diff = data.get("diff") or []
        if not diff:
            break
        out.extend(diff)
        if len(out) >= int(data.get("total") or 0) or pn > 60:
            break
        pn += 1
    return out


def _kline_sina(code):
    """新浪日K -> [(date, close, vol股, amt元, high, low)] 由旧到新"""
    _throttle()
    sym = tx_symbol(code)
    txt = _get("money.finance.sina.com.cn",
               "/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?" + urllib.parse.urlencode(
                   {"symbol": sym, "scale": "240", "ma": "no", "datalen": KLINE_N}),
               timeout=12, retries=2,
               hdr={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"}, raw=True)
    s = txt.strip()
    if not s or s[0] not in "[{":
        raise RuntimeError("sina bad payload")
    try:
        arr = json.loads(s)
    except Exception:
        arr = json.loads(s.replace("day:", '"day":').replace("open:", '"open":')
                         .replace("high:", '"high":').replace("low:", '"low":')
                         .replace("close:", '"close":').replace("volume:", '"volume":'))
    rows = []
    for it in arr:
        try:
            cl = float(it["close"]); vol = float(it["volume"]); hi = float(it["high"]); lo = float(it["low"])
            rows.append((it["day"][:10], cl, vol, vol * cl, hi, lo))
        except Exception:
            continue
    return rows


def _kline_tx(code):
    """腾讯日K(备源) -> [(date, close, vol手, amt元, high, low)]"""
    _throttle()
    sym = tx_symbol(code)
    d = _get("web.ifzq.gtimg.cn", "/appstock/app/fqkline/get?" + urllib.parse.urlencode(
        {"param": f"{sym},day,,,{KLINE_N},qfq"}), timeout=12, retries=2,
        hdr={"User-Agent": UA, "Referer": "https://gu.qq.com/"})
    node = ((d.get("data") or {}).get(sym)) or {}
    kl = node.get("qfqday") or node.get("day") or []
    rows = []
    for it in kl:
        try:
            # it: [日期, 开, 收, 高, 低, 量(手)]
            cl, op, hi, lo, vol = float(it[2]), float(it[1]), float(it[3]), float(it[4]), float(it[5])
            rows.append((it[0], cl, vol, vol * 100.0 * cl, hi, lo))
        except Exception:
            continue
    return rows


def _kline_em(code):
    """东财 push2his 个股日K(运行器IP可用; 沙箱IP被封) -> [(date,close,vol手,amt元,high,low)]"""
    _throttle()
    mkt = "1" if code.startswith(("60", "68", "9", "5")) else "0"
    secid = f"{mkt}.{code}"
    d = _get("push2his.eastmoney.com", "/api/qt/stock/kline/get?" + urllib.parse.urlencode({
        "secid": secid, "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101", "fqt": "1", "lmt": KLINE_N, "end": "20500101",
    }), timeout=12, retries=2, hdr={
        "User-Agent": UA, "Referer": "https://data.eastmoney.com/bkzj/hy.html",
        "Connection": "keep-alive", "Accept": "*/*"})
    kl = ((d.get("data") or {}).get("klines")) or []
    rows = []
    for s in kl:
        p = s.split(",")
        if len(p) < 7:
            continue
        try:
            cl, vol, amt = float(p[2]), float(p[5]), float(p[6])   # 额=元(真实成交)
            hi, lo = float(p[3]), float(p[4])
            rows.append((p[0], cl, vol, amt, hi, lo))
        except Exception:
            continue
    return rows


def cache_path(code, today):
    return os.path.join(CACHE_DIR, CACHE_VER, today, f"{code}.json")


def load_cache(code, today):
    """只读本地缓存, 无缓存返回 None"""
    cp = cache_path(code, today)
    if not os.path.exists(cp):
        return None
    try:
        with open(cp, encoding="utf-8") as f:
            return [tuple(x) for x in json.load(f)]
    except Exception:
        return None


def fetch_kline(code, today=None):
    """带本地缓存的日K获取: 新浪优先, 失败回落腾讯"""
    today = today or beijing_today()
    cp = cache_path(code, today)
    if USE_CACHE and os.path.exists(cp):
        try:
            with open(cp, encoding="utf-8") as f:
                return [tuple(x) for x in json.load(f)]
        except Exception:
            pass
    rows = None
    for fn in (_kline_em, _kline_sina, _kline_tx):
        try:
            r = fn(code)
            if r:
                rows = r
                break
        except Exception:
            continue
    if rows is None:
        return None                     # 网络失败: 不写缓存, 下轮可重试
    if USE_CACHE:                       # 含次新股等短K线, 也写入避免重复拉取
        try:
            os.makedirs(os.path.dirname(cp), exist_ok=True)
            with open(cp, "w", encoding="utf-8") as f:
                json.dump(rows, f)
        except Exception:
            pass
    return rows


def beijing_today():
    return (datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(hours=8)).date().isoformat()


def analyze(code, name, rows, today):
    """只做口径计算, 不做过滤; 返回 None 表示数据不足或基准额不达标"""
    if rows and rows[-1][0] == today:      # 剔除盘中未收盘的当日K线
        rows = rows[:-1]
    if len(rows) < 22:
        return None

    t, t1 = rows[-1], rows[-2]
    base = rows[-22:-2]                     # T-21 ~ T-2 共20根
    if len(base) != 20:
        return None

    base_avg = sum(r[3] for r in base) / 20.0
    # 条件1(更正): 基准期20个交易日【每一天】的成交额都 < QUIET_AMT_YI 亿
    if base_avg <= 0 or any(r[3] >= QUIET_AMT_YI * 1e8 for r in base):
        return None

    # 条件2: 前20个交易日平均股价 t, 每日最高<1.08t 且 最低>0.92t (±8%窄幅平台)
    t_price = sum(r[1] for r in base) / 20.0
    hi_lim, lo_lim = BAND_HI * t_price, BAND_LO * t_price
    for r in base:
        if r[4] >= hi_lim or r[5] <= lo_lim:     # r[4]=最高 r[5]=最低
            return None

    mult_t, mult_t1 = t[3] / base_avg, t1[3] / base_avg
    pct_t = (t[1] / t1[1] - 1.0) * 100.0
    pct_t1 = (t1[1] / rows[-3][1] - 1.0) * 100.0 if rows[-3][1] > 0 else 0.0

    return {
        "代码": code, "名称": name, "日期": t[0],
        "收盘": round(t[1], 2),
        "20日均价": round(t_price, 2),
        "T涨幅%": round(pct_t, 2),
        "T-1涨幅%": round(pct_t1, 2),
        "T日额(亿)": round(t[3] / 1e8, 3),
        "T-1额(亿)": round(t1[3] / 1e8, 3),
        "20日均额(亿)": round(base_avg / 1e8, 3),
        "T日倍数": round(mult_t, 2),
        "T-1倍数": round(mult_t1, 2),
        "基准区间": f"{base[0][0]}~{base[-1][0]}",
    }


def match(rec, mode):
    """A=两日均放量+T涨5%  B=仅T日放量+T涨5%  C=两日均放量+两日均涨5%"""
    if not rec:
        return False
    ok_vol = rec["T日倍数"] >= VOL_MULT
    ok_vol2 = ok_vol and rec["T-1倍数"] >= VOL_MULT
    ok_up = rec["T涨幅%"] >= PCT_UP
    ok_up2 = ok_up and rec["T-1涨幅%"] >= PCT_UP
    if mode == "A":
        return ok_vol2 and ok_up
    if mode == "B":
        return ok_vol and ok_up
    if mode == "C":
        return ok_vol2 and ok_up2
    return False


def build_markdown(today, buckets, prefiltered, scanned):
    MODES = [
        ("A", f"严格档：T日与T-1日均放量≥{VOL_MULT}x 且 T日涨≥{PCT_UP}%"),
        ("B", f"宽松档：仅T日放量≥{VOL_MULT}x 且 T日涨≥{PCT_UP}%"),
        ("C", f"最严档：T日与T-1日均放量≥{VOL_MULT}x 且 两日均涨≥{PCT_UP}%"),
    ]
    L = []
    L.append(f"# 📊 A股放量突破选股 · {today}")
    L.append("")
    L.append(f"**筛选口径**：前20个交易日（T-21~T-2）【逐日】成交额 < {QUIET_AMT_YI}亿，"
             f"且20日均价t处于 ±{int((1-BAND_LO)*100)}% 窄幅平台；"
             f"T与T-1日放量 ≥ {VOL_MULT}x；T日涨幅 ≥ {PCT_UP}%。")
    L.append(f"（已初筛排除最新单日成交额≥{PRE_FILTER_YI}亿的 {prefiltered} 只，进入扫描 {scanned} 只）")
    L.append("")
    cols = ["代码", "名称", "收盘", "20日均价", "T涨幅%", "T日额(亿)", "T日倍数", "基准区间"]
    for m, desc in MODES:
        b = sorted(buckets[m], key=lambda x: (-x["T日倍数"], -x["T涨幅%"]))
        L.append(f"## {m}档 · {desc}")
        L.append(f"**共 {len(b)} 只**")
        if b:
            L.append("| " + " | ".join(cols) + " |")
            L.append("|" + "|".join(["---"] * len(cols)) + "|")
            for r in b:
                L.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
        else:
            L.append("（无命中）")
        L.append("")
    return "\n".join(L) + "\n"


def main():
    today = beijing_today()
    print(f"[1/3] 拉全市场A股清单 (今日={today}, T=最近完整交易日)")
    uni = fetch_universe()
    pool, prefiltered = [], 0
    for it in uni:
        code, name = str(it.get("f12") or ""), str(it.get("f14") or "")
        if not code:
            continue
        if EXCLUDE_ST and ("ST" in name.upper() or "退" in name):
            continue
        if EXCLUDE_BJ and code.startswith(("8", "4")):
            continue
        # 初筛: 最新单日成交额 >= PRE_FILTER_YI 亿的必非安静股, 直接排除(省去kline请求)
        try:
            amt = float(it.get("f6") or 0)
        except Exception:
            amt = 0
        if amt >= PRE_FILTER_YI * 1e8:
            prefiltered += 1
            continue
        pool.append((code, name))
    print(f"      初筛后待扫描 {len(pool)} 只 (已排除ST/退市/北交所); "
          f"另按最新额>={PRE_FILTER_YI}亿初筛排除 {prefiltered} 只")

    # ---- 阶段2a: 补缓存(可分批, 命令行参数=本轮最多联网只数; 0=不限) ----
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    todo = [it for it in pool if not (USE_CACHE and os.path.exists(cache_path(it[0], today)))]
    print(f"[2/3] 补日K缓存: 已有 {len(pool)-len(todo)} 只, 待补 {len(todo)} 只")
    if limit and len(todo) > limit:
        todo = todo[:limit]
        print(f"      本轮限量 {limit} 只 (多轮累积, 重跑自动续)")

    scanned, fail = 0, 0
    t0 = time.time()
    consec_fail = 0

    if todo:
        for item in todo:
            code, name = item
            try:
                rows = fetch_kline(code, today)
            except Exception:
                rows = None
            scanned += 1
            if rows is None:
                fail += 1
                consec_fail += 1
                # 自适应冷却: 连续失败说明被限流, 退避越来越长
                if consec_fail == 5:
                    print(f"      ⚠ 连续失败{consec_fail}, 冷却30s...")
                    time.sleep(30)
                elif consec_fail == 15:
                    print(f"      ⚠ 连续失败{consec_fail}, 冷却90s...")
                    time.sleep(90)
                elif consec_fail >= 30:
                    print(f"      ⚠ 连续失败{consec_fail}, 冷却180s...")
                    time.sleep(180)
            else:
                consec_fail = 0
            if scanned % 200 == 0:
                print(f"      已试 {scanned}/{len(todo)}  成功{scanned-fail}  失败{fail}  {time.time()-t0:.0f}s")
        print(f"      本轮完成 {scanned} 只, 成功 {scanned-fail}, 失败 {fail}, 耗时 {time.time()-t0:.0f}s")

    # ---- 阶段2b: 基于全部缓存做分析 ----
    recs, missing = [], 0
    for code, name in pool:
        rows = load_cache(code, today)
        if rows is None:
            missing += 1
            continue
        r = analyze(code, name, rows, today)
        if r:
            recs.append(r)
    print(f"      缓存覆盖 {len(pool)-missing}/{len(pool)} 只 (缺 {missing})")
    print(f"      其中 [基准期20日逐日均额<{QUIET_AMT_YI}亿] 共 {len(recs)} 只, 进入三档比对")

    MODES = [
        ("A", f"T日与T-1日均放量>={VOL_MULT}x 且 T日涨>={PCT_UP}%  (严格档)"),
        ("B", f"仅T日放量>={VOL_MULT}x 且 T日涨>={PCT_UP}%        (宽松档)"),
        ("C", f"T日与T-1日均放量>={VOL_MULT}x 且 两日均涨>={PCT_UP}% (最严档)"),
    ]
    buckets = {}
    print(f"\n[3/3] 三档比对结果")
    for m, desc in MODES:
        b = [r for r in recs if match(r, m)]
        buckets[m] = b
        print(f"      {m}档 {len(b):3d} 只   {desc}")

    cols = ["代码", "名称", "日期", "收盘", "20日均价", "T涨幅%", "T-1涨幅%", "T日额(亿)",
            "T-1额(亿)", "20日均额(亿)", "T日倍数", "T-1倍数"]
    for m, desc in MODES:
        b = sorted(buckets[m], key=lambda x: (-x["T日倍数"], -x["T涨幅%"]))
        print(f"\n===== {m}档: {desc} =====  共 {len(b)} 只")
        if b:
            w = {c: max(len(c), max(len(str(r[c])) for r in b)) for c in cols}
            print("  " + "  ".join(c.ljust(w[c]) for c in cols))
            print("  " + "-" * (sum(w.values()) + 2 * len(cols)))
            for r in b:
                print("  " + "  ".join(str(r[c]).ljust(w[c]) for c in cols))

    for m, _ in MODES:
        b = sorted(buckets[m], key=lambda x: (-x["T日倍数"], -x["T涨幅%"]))
        out = f"screen_result_{m}.csv"
        with open(out, "w", newline="", encoding="utf-8-sig") as f:
            if b:
                wtr = csv.DictWriter(f, fieldnames=list(b[0].keys()))
                wtr.writeheader()
                wtr.writerows(b)
        print(f"已写入 {out}  ({len(b)} 只)")

    # ---- 生成微信 markdown 报告 ----
    md = build_markdown(today, buckets, prefiltered, len(pool))
    with open("screen_result.md", "w", encoding="utf-8") as f:
        f.write(md)
    print(f"已写入 screen_result.md (供微信推送)")

    print(f"\n口径: 基准20日(T-21~T-2)【逐日】均额<{QUIET_AMT_YI}亿 且 20日均价t的±{int((1-BAND_LO)*100)}%窄幅平台"
          f" | T与T-1放量>={VOL_MULT}x | T日涨>={PCT_UP}%")


if __name__ == "__main__":
    main()
