#!/usr/bin/env python3
"""并发写入准入压测: 单批首次越线后, 并行等待的批次必须按数据库新实况被拒。

每轮不变量(阈值 = 当前保留 + 单批增量/2, 恰容一批):
  - 并行 N 批写入中恰有 1 批被受理(首次越线批);
  - 其余 N-1 批全部被拒(409 threshold_exceeded)且一行不落;
  - demo_events 净增 == 恰一批的行数(无半批、无漏判)。
设置阈值/写入交错时, 准入必须对应实际写入时的代次与阈值 —— 由数据库行锁
串行化保证, 与服务进程数无关; 设 BASE_URL_2 可用两个进程真并发验证。

环境变量:
  BASE_URL    默认 http://127.0.0.1:8000
  BASE_URL_2  可选第二服务进程地址(双进程并行写入)
  ROUNDS      默认 10
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
BASE2 = os.environ.get("BASE_URL_2", "").rstrip("/")
ROUNDS = int(os.environ.get("ROUNDS", "10"))
ROWS = 500
PAR = 4  # 每轮并行批数


def req_to(base, method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(base + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}


def req(method, path, body=None):
    return req_to(BASE, method, path, body)


def status():
    code, j = req("GET", "/api/status")
    assert code == 200 and j.get("db_ok"), f"status 异常: {code} {j}"
    return j


def main():
    st = status()
    created_here = False
    if not st["slot"]["exists"]:
        code, j = req("POST", "/api/slot/create")
        assert code == 200 and j.get("ok"), f"create 失败: {j}"
        created_here = True
        print(f"已创建演示槽(代 {j['generation']})")

    # 估单批 WAL 增量
    r0 = status()["slot"]["retained_bytes"]
    code, j = req("POST", "/api/demo/write", {"rows": ROWS})
    assert code == 200, j
    per_batch = status()["slot"]["retained_bytes"] - r0
    assert per_batch > 0, "单批 WAL 增量应大于 0"
    print(f"单批 {ROWS} 行 ≈ {per_batch} B WAL; 并行度 {PAR}; "
          f"目标: {BASE}" + (f" + {BASE2}" if BASE2 else "(单进程)"))

    fails = 0
    for i in range(1, ROUNDS + 1):
        r1 = status()["slot"]["retained_bytes"]
        thr = r1 + per_batch // 2  # 当前未越线; 恰一批后越线
        code, j = req("POST", "/api/slot/threshold", {"threshold_bytes": thr})
        assert code == 200 and j.get("ok"), f"设置阈值失败: {j}"
        before = status()["demo_events"]

        bases = [BASE, BASE2] if BASE2 else [BASE]
        results = []

        def worker(b):
            results.append(req_to(b, "POST", "/api/demo/write", {"rows": ROWS}))

        threads = [threading.Thread(target=worker, args=(bases[k % len(bases)],))
                   for k in range(PAR)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        oks = [r for r in results if r[0] == 200 and r[1].get("ok")]
        rejs = [r for r in results
                if r[0] == 409 and r[1].get("reason") == "threshold_exceeded"]
        grew = status()["demo_events"] - before
        ok = len(oks) == 1 and len(rejs) == PAR - 1 and grew == ROWS
        print(f"  轮次 {i:2d}: 成功 {len(oks)} 拒绝 {len(rejs)} "
              f"净增 {grew} 行  {'OK' if ok else 'VIOLATION'}")
        if not ok:
            fails += 1
            for r in results:
                print("    ", r[0], {k: v for k, v in r[1].items()
                                     if k in ("ok", "error", "reason", "inserted")})

    code, j = req("POST", "/api/slot/threshold", {"threshold_bytes": None})
    assert code == 200, j
    if created_here:
        # 清理自己创建的槽, 恢复干净环境(等租约过期后回收)
        time.sleep(6)
        code, j = req("POST", "/api/slot/reclaim")
        assert code == 200 and j.get("reclaimed"), f"清理回收失败: {j}"
        print("已清理演示槽")
    print(f"\n== 并发写入准入: {ROUNDS - fails}/{ROUNDS} 轮满足不变量 ==")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
