# -*- coding: utf-8 -*-
"""云端数据源探针: 实测 em/sina/tx 三源在 GitHub 运行器上对单只日K的抓取耗时/成功率,
用于诊断选股器为何在 push2his 逐只抓取时极慢/疑似卡死。"""
import time
import sys
import screen_volume_breakout as S

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 探针用更激进超时, 避免被单源挂起拖累整体
S.EM_TIMEOUT = 4
S.EM_RETRIES = 0
S.THROTTLE = 0.0
S.WORKERS = 1


def main():
    print("[probe] 拉取全市场清单 ...")
    uni = S.fetch_universe()
    cands = []
    for it in uni:
        code = str(it.get("f12") or "")
        name = str(it.get("f14") or "")
        if not code:
            continue
        if S.EXCLUDE_ST and ("ST" in name.upper() or "退" in name):
            continue
        if S.EXCLUDE_BJ and code.startswith(("8", "4")):
            continue
        cands.append((code, name))
        if len(cands) >= 50:
            break
    print(f"[probe] 取 {len(cands)} 只样本")

    srcs = [("em", S._kline_em), ("sina", S._kline_sina), ("tx", S._kline_tx)]
    out = [f"PROBE @ {time.strftime('%Y-%m-%d %H:%M:%S')}  样本={len(cands)}"]
    for key, fn in srcs:
        ok = fail = empty = 0
        ts = []
        worst = ("", 0.0)
        for code, name in cands:
            t0 = time.time()
            try:
                r = fn(code)
            except Exception:
                r = None
            dt = time.time() - t0
            if r is None:
                fail += 1
            elif len(r) == 0:
                empty += 1
            else:
                ok += 1
                ts.append(dt)
                if dt > worst[1]:
                    worst = (code, dt)
        n = ok + fail
        avg = (sum(ts) / len(ts) * 1000) if ts else 0.0
        mx = (max(ts) * 1000) if ts else 0.0
        ser = avg / 1000 * n if n else 0.0
        par = ser / 8 if ser else 0.0
        out.append(f"\n=== 源 {key} ===  ok={ok} fail={fail} empty={empty}")
        out.append(f"  平均耗时={avg:.0f}ms  最大耗时={mx:.0f}ms  最慢样本={worst[0]}({worst[1]*1000:.0f}ms)")
        out.append(f"  推算全市场安静股(约3500只)串行≈{ser:.0f}s; 若8线程并发≈{par:.0f}s")
    txt = "\n".join(out) + "\n"
    with open("probe_result.txt", "w", encoding="utf-8") as f:
        f.write(txt)
    print(txt)
    print("[probe] 完成, 见 probe_result.txt")


if __name__ == "__main__":
    main()
