# -*- coding: utf-8 -*-
"""用新浪 hq.sinajs.cn 枚举全A代码空间, 生成 data/universe.csv (code,name,f6成交额元).
不依赖东财(已封). 仅作为选股器 universe 的静态兜底源."""
import csv, sys, threading, time, urllib.request, urllib.parse

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REFERER = "https://finance.sina.com.cn/"

# 枚举 A 股代码空间(6位)
def code_ranges():
    rngs = []
    # 沪市主板 600-605
    for p in (600, 601, 603, 605):
        rngs += [f"sh{p}{i:03d}" for i in range(1000)]
    # 科创板 688
    rngs += [f"sh688{i:03d}" for i in range(1000)]
    # 深市主板 000-003
    for p in (0, 1, 2, 3):
        rngs += [f"sz{p:03d}{i:03d}" for i in range(1000)]
    # 创业板 300-301
    for p in (300, 301):
        rngs += [f"sz{p}{i:03d}" for i in range(1000)]
    # 北交所 83/87/88/89/92
    for p in (830, 831, 832, 833, 834, 835, 836, 837, 838, 839,
             870, 871, 872, 873, 874, 875, 876, 877, 878, 879,
             880, 881, 882, 883, 884, 885, 886, 887, 888, 889,
             920, 921):
        rngs += [f"bj{p}{i:03d}" for i in range(1000)]
    return rngs

_lock = threading.Lock()
out = []  # (code, name, f6)

def fetch_batch(syms):
    url = "https://hq.sinajs.cn/list=" + ",".join(syms)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": REFERER})
        data = urllib.request.urlopen(req, timeout=12).read().decode("gbk", "ignore")
    except Exception as e:
        return
    for line in data.split(";"):
        line = line.strip()
        if not line.startswith("var hq_str_"):
            continue
        try:
            sym = line[len("var hq_str_"):line.index("=")]
            body = line[line.index('"')+1:line.rindex('"')]
            if not body:
                continue
            f = body.split(",")
            name = f[0]
            if not name or "抱歉" in name:
                continue
            # f[9] = 成交额(元) 字段
            amt = 0.0
            try:
                amt = float(f[9])
            except Exception:
                amt = 0.0
            with _lock:
                out.append((sym[2:], name, amt))
        except Exception:
            continue

def main():
    syms = code_ranges()
    print(f"枚举候选 {len(syms)} 个代码, 分批(每批80)查询新浪...", flush=True)
    batch, B = [], 80
    threads = []
    t0 = time.time()
    def runner(bs):
        # 串行批内, 线程间并行
        for b in bs:
            fetch_batch(b)
    # 简单线程池
    import concurrent.futures as cf
    batches = [syms[i:i+B] for i in range(0, len(syms), B)]
    with cf.ThreadPoolExecutor(max_workers=12) as ex:
        list(ex.map(fetch_batch, batches))
    print(f"完成: 有效股票 {len(out)} 只, 耗时 {time.time()-t0:.0f}s", flush=True)
    out.sort(key=lambda x: x[0])
    import os
    os.makedirs("data", exist_ok=True)
    with open("data/universe.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["code", "name", "f6"])
        for r in out:
            w.writerow(r)
    print("已写入 data/universe.csv", flush=True)

if __name__ == "__main__":
    main()
