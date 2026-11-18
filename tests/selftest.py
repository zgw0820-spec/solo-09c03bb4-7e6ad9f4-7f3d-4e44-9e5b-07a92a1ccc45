#!/usr/bin/env python3
"""CDC 复制槽回收值班台 — 端到端自测(仅用标准库)。

覆盖场景:
  1. 创建演示槽 / 重复创建拒绝
  2. 续租 + 租约有效时回收被拒(续租先成功 -> 不得删除)
  3. 租约过期后回收成功; 旧续租被拒(回收先成功 -> 旧续租失败)
  4. 同名重建 -> 旧代续租不得影响新槽
  5. 模拟写入 -> 保留 WAL 字节增长; 其他写入不受回收影响
  6. 数据库正在使用槽(pg_recvlogical 真实消费) -> 回收被拒且不删除、可再续租
  7. 崩溃恢复: tombstone 遗留时回收能继续完成删除
  8. 服务重启后按数据库实况显示, 不谎报
  9. WAL 软阈值: 设置/显示/达阈值整批拒绝(一行不落)/被阻断时续租可用/清除恢复
 10. 并发写入: 单批首次越线后, 并行等待的下一批按新实况重判被拒
 11. 故障自闭: 数据库故障或槽缺失(保留量不可核实)时不放行, 恢复后据实重判
 12. 代次隔离: 回收清除阈值, 同名重建的新一代不继承阈值与阻断结论
环境变量:
  BASE_URL         默认 http://127.0.0.1:8000
  BASE_URL_2       可选第二服务进程地址(双进程并行写入用例)
  PGBIN            本地 PostgreSQL bin 目录(含 pg_recvlogical/psql)
  APP_RESTART_CMD  重启应用的命令(可选; 不设则跳过重启用例)
  DB_STOP_CMD      停止数据库的命令(可选; 不设则跳过故障自闭用例)
  DB_START_CMD     启动数据库的命令(可选)
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
BASE2 = os.environ.get("BASE_URL_2", "").rstrip("/")
PGBIN = os.environ.get("PGBIN", os.path.expanduser("~/cdc-runtime/pg16/bin"))
PG = dict(host="127.0.0.1", port=os.environ.get("PGPORT", "5432"),
          user="postgres", db="appdb")
RESTART_CMD = os.environ.get("APP_RESTART_CMD")
DB_STOP_CMD = os.environ.get("DB_STOP_CMD")
DB_START_CMD = os.environ.get("DB_START_CMD")

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def req(method, path, body=None):
    return req_to(BASE, method, path, body)


def req_to(base, method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(base + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:  # 连接失败等: 如实返回, 由调用方判定
        return -1, {"ok": False, "error": f"connect_failed: {e}"}


def wait_app(timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            code, j = req("GET", "/api/status")
            if code == 200 and j.get("db_ok"):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def status():
    code, j = req("GET", "/api/status")
    assert code == 200 and j.get("db_ok"), f"status 异常: {code} {j}"
    return j


def psql(sql):
    r = subprocess.run(
        [os.path.join(PGBIN, "psql"), "-h", PG["host"], "-p", PG["port"],
         "-U", PG["user"], "-d", PG["db"], "-X", "-c", sql],
        capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, f"psql 失败: {r.stderr}"
    return r.stdout


def main():
    print(f"== 目标 {BASE} ==")
    assert wait_app(), "应用未就绪"
    print("== 应用就绪 ==")

    # ---------- 1. 初始状态 ----------
    st = status()
    check("初始: 槽不存在", st["slot"]["exists"] is False, st)
    check("初始: 无租约", st["lease"] is None, st)

    # ---------- 2. 创建 ----------
    code, j = req("POST", "/api/slot/create")
    check("创建演示槽", code == 200 and j.get("ok") and j.get("generation") == 1, j)
    st = status()
    check("创建后: 槽存在", st["slot"]["exists"] is True, st)
    check("创建后: 槽空闲(active=false)", st["slot"]["active"] is False, st)
    check("创建后: 保留 WAL 为数值", isinstance(st["slot"]["retained_bytes"], int), st)
    check("创建后: 租约 active 且未过期",
          st["lease"] and st["lease"]["state"] == "active" and not st["lease"]["expired"], st)
    check("创建后: 租约约 5 秒", 3000 < st["lease"]["remaining_ms"] <= 5200, st["lease"])

    code, j = req("POST", "/api/slot/create")
    check("重复创建被拒(409)", code == 409 and j.get("error") == "slot_exists", j)

    # ---------- 3. 续租先成功 -> 回收不得删除 ----------
    code, j = req("POST", "/api/slot/renew", {"generation": 1})
    check("续租(代1)成功", code == 200 and j.get("ok"), j)
    check("续租后剩余约 5 秒", 4000 < j.get("remaining_ms", 0) <= 5200, j)
    code, j = req("POST", "/api/slot/reclaim")
    check("租约有效时回收被拒(409 lease_active)",
          code == 409 and j.get("error") == "lease_active", j)
    st = status()
    check("被拒后槽仍存在", st["slot"]["exists"] is True, st)

    code, j = req("POST", "/api/slot/renew", {"generation": 999})
    check("错误代次续租被拒(409 generation_mismatch)",
          code == 409 and j.get("reason") == "generation_mismatch", j)

    # ---------- 4. 过期后回收成功 -> 旧续租失败 ----------
    print("  … 等待租约过期(6s)")
    time.sleep(6)
    code, j = req("POST", "/api/slot/reclaim")
    check("过期后回收成功", code == 200 and j.get("ok") and j.get("reclaimed"), j)
    st = status()
    check("回收后槽不存在", st["slot"]["exists"] is False, st)
    check("回收后租约为 reclaimed", st["lease"] and st["lease"]["state"] == "reclaimed", st)
    code, j = req("POST", "/api/slot/renew", {"generation": 1})
    check("回收后旧续租被拒(409 lease_reclaimed)",
          code == 409 and j.get("reason") == "lease_reclaimed", j)

    # ---------- 5. 同名重建 -> 旧代不得影响新槽 ----------
    code, j = req("POST", "/api/slot/create")
    check("同名重建成功(代=2)", code == 200 and j.get("generation") == 2, j)
    code, j = req("POST", "/api/slot/renew", {"generation": 1})
    check("旧代续租被拒(409)", code == 409 and j.get("reason") == "generation_mismatch", j)
    code, j = req("POST", "/api/slot/renew", {"generation": 2})
    check("新代续租成功", code == 200 and j.get("ok"), j)
    st = status()
    check("新槽未被旧代操作影响(仍存在)", st["slot"]["exists"] is True, st)

    # ---------- 6. 模拟写入 -> 保留 WAL 增长 ----------
    code, j = req("POST", "/api/demo/write", {"rows": 200})
    check("模拟写入 200 行", code == 200 and j.get("inserted") == 200, j)
    r1 = status()["slot"]["retained_bytes"]
    code, j = req("POST", "/api/demo/write", {"rows": 500})
    check("模拟写入 500 行", code == 200 and j.get("inserted") == 500, j)
    r2 = status()["slot"]["retained_bytes"]
    check("保留 WAL 字节增长", r2 > r1, f"{r1} -> {r2}")

    # ---------- 7. 数据库正在使用槽 -> 回收失败且不删除 ----------
    print("  … 等待租约过期(6s)")
    time.sleep(6)
    recvlog = os.path.join(PGBIN, "pg_recvlogical")
    proc = subprocess.Popen(
        [recvlog, "-h", PG["host"], "-p", PG["port"], "-U", PG["user"],
         "-d", PG["db"], "-S", "cdc_demo_slot", "--start",
         "-o", "proto_version=1", "-o", "publication_names=cdc_pub",
         "-f", "/dev/null"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        active = False
        for _ in range(30):
            if status()["slot"].get("active"):
                active = True
                break
            time.sleep(0.5)
        check("消费者接入后槽 active=true", active, status()["slot"])
        code, j = req("POST", "/api/slot/reclaim")
        check("槽使用中回收被拒(409 slot_active)",
              code == 409 and j.get("error") == "slot_active", j)
        st = status()
        check("回收失败后槽仍存在", st["slot"]["exists"] is True, st)
        check("回收失败后租约回滚为 active(未谎报)",
              st["lease"] and st["lease"]["state"] == "active", st["lease"])
        code, j = req("POST", "/api/slot/renew", {"generation": 2})
        check("回收失败后消费者可再续租", code == 200 and j.get("ok"), j)
        code, j = req("POST", "/api/demo/write", {"rows": 50})
        check("回收失败后其他写入仍可执行", code == 200 and j.get("inserted") == 50, j)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    inactive = False
    for _ in range(40):
        if not status()["slot"].get("active"):
            inactive = True
            break
        time.sleep(0.5)
    check("消费者断开后槽 active=false", inactive, status()["slot"])

    print("  … 等待租约过期(6s)")
    time.sleep(6)
    code, j = req("POST", "/api/slot/reclaim")
    check("消费者断开后回收成功", code == 200 and j.get("reclaimed"), j)
    check("槽已删除", status()["slot"]["exists"] is False)

    # ---------- 8. 崩溃恢复: tombstone 遗留 -> 回收继续完成 ----------
    code, j = req("POST", "/api/slot/create")
    check("再次创建(代=3)", code == 200 and j.get("generation") == 3, j)
    psql("UPDATE cdc_slot_lease SET state='reclaimed'")  # 模拟崩溃遗留的 tombstone
    st = status()
    check("实况: tombstone 遗留且槽仍存在",
          st["lease"]["state"] == "reclaimed" and st["slot"]["exists"], st)
    code, j = req("POST", "/api/slot/reclaim")
    check("恢复路径回收成功", code == 200 and j.get("reclaimed"), j)
    check("恢复后槽不存在", status()["slot"]["exists"] is False)

    # ---------- 9. 服务重启: 按实况显示, 不丢不谎报 ----------
    code, j = req("POST", "/api/slot/create")
    check("重启前创建(代=4)", code == 200 and j.get("generation") == 4, j)
    if RESTART_CMD:
        r = subprocess.run(RESTART_CMD, shell=True, timeout=120)
        check("应用重启命令执行成功", r.returncode == 0, r)
        assert wait_app(), "重启后应用未就绪"
        st = status()
        check("重启后槽仍存在(实况)", st["slot"]["exists"] is True, st)
        check("重启后租约未丢(代=4, active)",
              st["lease"] and st["lease"]["generation"] == 4
              and st["lease"]["state"] == "active", st["lease"])
        print("  … 等待租约过期(6s)")
        time.sleep(6)
        code, j = req("POST", "/api/slot/reclaim")
        check("重启后回收仍可用", code == 200 and j.get("reclaimed"), j)
    else:
        print("  … 跳过重启用例(未设 APP_RESTART_CMD)")
        print("  … 等待租约过期(6s)")
        time.sleep(6)
        code, j = req("POST", "/api/slot/reclaim")
        check("跳过重启用例后清理回收", code == 200 and j.get("reclaimed"), j)

    # ---------- 10. WAL 软阈值: 设置/显示/越线整批拒绝/清除恢复 ----------
    st = status()
    if st["slot"]["exists"]:  # 确保干净起点
        print("  … 清理残留槽(等待过期 6s)")
        time.sleep(6)
        code, j = req("POST", "/api/slot/reclaim")
        assert code == 200 and j.get("reclaimed"), f"清理失败: {j}"
    code, j = req("POST", "/api/slot/create")
    gen = j.get("generation")
    check("阈值用例: 新建槽", code == 200 and j.get("ok"), j)

    st = status()
    adm = st["admission"]
    check("未设阈值: 准入不适用且放行",
          adm["applicable"] is False and adm["allowed"] is True
          and adm["reason"] == "no_threshold", adm)
    code, j = req("POST", "/api/demo/write", {"rows": 100})
    check("未设阈值: 写入放行(原有行为)", code == 200 and j.get("inserted") == 100, j)

    r_now = status()["slot"]["retained_bytes"]
    # 余量要覆盖"设置阈值"这条 UPDATE 自身产生的 WAL(约百字节),
    # 同时远小于一批 300 行的增量(约 30KB), 保证: 当前未越线, 一批后越线。
    thr = r_now + 8192
    code, j = req("POST", "/api/slot/threshold", {"threshold_bytes": thr})
    check("设置阈值成功(作用于当前代)",
          code == 200 and j.get("ok") and j.get("threshold_bytes") == thr
          and j.get("generation") == gen, j)
    st = status()
    adm = st["admission"]
    check("状态: 显示阈值与实测字节数",
          adm["applicable"] is True and adm["threshold_bytes"] == thr
          and isinstance(adm["retained_bytes"], int), adm)
    check("状态: 未越线时准入放行",
          adm["allowed"] is True and adm["reason"] == "under_threshold", adm)

    code, j = req("POST", "/api/demo/write", {"rows": 300})
    check("受理时未达阈值: 首次越线的一批被受理", code == 200 and j.get("inserted") == 300, j)
    st = status()
    check("越线后状态转为拒绝",
          st["admission"]["allowed"] is False
          and st["admission"]["reason"] == "threshold_reached", st["admission"])
    before = st["demo_events"]
    code, j = req("POST", "/api/demo/write", {"rows": 300})
    check("达阈值: 整批拒绝(409)",
          code == 409 and j.get("error") == "write_rejected"
          and j.get("reason") == "threshold_exceeded" and j.get("inserted") == 0, j)
    check("达阈值: 整批不落行",
          status()["demo_events"] == before,
          f"{before} -> {status()['demo_events']}")

    code, j = req("POST", "/api/slot/renew", {"generation": gen})
    check("写入被阻断时续租仍可用", code == 200 and j.get("ok"), j)

    code, j = req("POST", "/api/slot/threshold", {"threshold_bytes": None})
    check("清除阈值", code == 200 and j.get("ok") and j.get("threshold_bytes") is None, j)
    code, j = req("POST", "/api/demo/write", {"rows": 50})
    check("清除阈值后恢复放行", code == 200 and j.get("inserted") == 50, j)

    # ---------- 11. 并发写入: 越线后等待批次按新实况重判 ----------
    r0 = status()["slot"]["retained_bytes"]
    code, j = req("POST", "/api/demo/write", {"rows": 500})
    assert code == 200 and j.get("inserted") == 500, j
    r1 = status()["slot"]["retained_bytes"]
    per_batch = r1 - r0
    check("测得单批 WAL 增量为正", per_batch > 0, f"{r0} -> {r1}")
    thr = r1 + per_batch // 2  # 当前未越线; 恰一批后越线
    code, j = req("POST", "/api/slot/threshold", {"threshold_bytes": thr})
    check("设置并发用例阈值", code == 200 and j.get("ok"), j)
    before = status()["demo_events"]

    bases = [BASE, BASE2] if BASE2 else [BASE, BASE]
    if not BASE2:
        print("  … 未设 BASE_URL_2, 两个并发请求发往同一进程(线程池并行)")
    results = []

    def worker(b):
        results.append(req_to(b, "POST", "/api/demo/write", {"rows": 500}))

    threads = [threading.Thread(target=worker, args=(b,)) for b in bases]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    oks = [r for r in results if r[0] == 200 and r[1].get("ok")]
    rejs = [r for r in results
            if r[0] == 409 and r[1].get("reason") == "threshold_exceeded"]
    check("并行两批: 恰一批越线被受理", len(oks) == 1, results)
    check("并行两批: 等待的一批按新实况被拒", len(rejs) == 1, results)
    check("并行: demo_events 只净增一批(无半批)",
          status()["demo_events"] == before + 500,
          f"{before} -> {status()['demo_events']}")

    # ---------- 11b. 回收失败回滚: 阈值按原样恢复 ----------
    print("  … 等待租约过期(6s)")
    time.sleep(6)
    recvlog = os.path.join(PGBIN, "pg_recvlogical")
    proc = subprocess.Popen(
        [recvlog, "-h", PG["host"], "-p", PG["port"], "-U", PG["user"],
         "-d", PG["db"], "-S", "cdc_demo_slot", "--start",
         "-o", "proto_version=1", "-o", "publication_names=cdc_pub",
         "-f", "/dev/null"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        active = False
        for _ in range(30):
            if status()["slot"].get("active"):
                active = True
                break
            time.sleep(0.5)
        check("消费者接入后槽 active=true(阈值用例)", active, status()["slot"])
        code, j = req("POST", "/api/slot/reclaim")
        check("槽使用中回收被拒(阈值用例)",
              code == 409 and j.get("error") == "slot_active", j)
        st = status()
        check("回收失败回滚后阈值按原样恢复",
              st["lease"] and st["lease"]["state"] == "active"
              and st["lease"].get("wal_threshold_bytes") == thr, st["lease"])
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    inactive = False
    for _ in range(40):
        if not status()["slot"].get("active"):
            inactive = True
            break
        time.sleep(0.5)
    check("消费者断开后槽 active=false(阈值用例)", inactive, status()["slot"])

    # 消费者期间会推进 restart_lsn 使保留量缩小, 重新制造越线状态供故障用例
    r_now = status()["slot"]["retained_bytes"]
    thr = r_now + 8192
    code, j = req("POST", "/api/slot/threshold", {"threshold_bytes": thr})
    check("重设越线阈值", code == 200 and j.get("ok"), j)
    code, j = req("POST", "/api/demo/write", {"rows": 300})
    check("重设后首批越线被受理", code == 200 and j.get("inserted") == 300, j)
    code, j = req("POST", "/api/demo/write", {"rows": 10})
    check("越线状态就绪(下一批被拒)",
          code == 409 and j.get("reason") == "threshold_exceeded", j)

    # ---------- 12. 故障自闭: 故障/不可核实不放行, 恢复后据实重判 ----------
    if DB_STOP_CMD and DB_START_CMD:
        r = subprocess.run(DB_STOP_CMD, shell=True, timeout=60)
        check("停止数据库", r.returncode == 0, r)
        code, j = req("POST", "/api/demo/write", {"rows": 10})
        check("数据库故障: 已设阈值的写入不放行",
              code in (503, -1) and not j.get("ok"), (code, j))
        code, j = req("GET", "/api/status")
        check("数据库故障: 状态如实 db_ok=false",
              code in (200, -1) and (code == -1 or j.get("db_ok") is False), (code, j))
        r = subprocess.run(DB_START_CMD, shell=True, timeout=120)
        check("重启数据库", r.returncode == 0, r)
        assert wait_app(), "数据库恢复后应用未就绪"
        code, j = req("POST", "/api/demo/write", {"rows": 10})
        check("恢复后据实重判: 仍越线仍拒",
              code == 409 and j.get("reason") == "threshold_exceeded", j)
        r_now = status()["slot"]["retained_bytes"]
        req("POST", "/api/slot/threshold", {"threshold_bytes": r_now + 10**9})
        code, j = req("POST", "/api/demo/write", {"rows": 10})
        check("恢复后据实重判: 提高阈值即放行", code == 200 and j.get("inserted") == 10, j)
        req("POST", "/api/slot/threshold", {"threshold_bytes": thr})  # 还原越线状态
    else:
        print("  … 跳过数据库故障用例(未设 DB_STOP_CMD/DB_START_CMD)")

    # 带外删槽: 租约仍 active 且带阈值, 但保留量不可核实 -> 不得放行
    psql("SELECT pg_drop_replication_slot('cdc_demo_slot')")
    st = status()
    check("槽缺失: 准入状态为不可核实并拒绝",
          st["admission"]["allowed"] is False
          and st["admission"]["reason"] == "slot_unverifiable", st["admission"])
    before = st["demo_events"]
    code, j = req("POST", "/api/demo/write", {"rows": 10})
    check("槽缺失: 已设阈值的写入不放行",
          code == 409 and j.get("reason") == "slot_unverifiable"
          and j.get("inserted") == 0, j)
    check("槽缺失: 整批不落行", status()["demo_events"] == before,
          f"{before} -> {status()['demo_events']}")

    # ---------- 13. 代次隔离: 回收清阈值, 重建不继承 ----------
    print("  … 等待租约过期(6s)")
    time.sleep(6)
    code, j = req("POST", "/api/slot/reclaim")
    check("回收仍可用(槽缺失的恢复路径)", code == 200 and j.get("reclaimed"), j)
    st = status()
    check("回收后租约不再携带阈值",
          st["lease"] and st["lease"].get("wal_threshold_bytes") is None, st["lease"])
    code, j = req("POST", "/api/slot/threshold", {"threshold_bytes": 1024})
    check("已回收代设置阈值被拒", code == 409 and j.get("reason") == "lease_reclaimed", j)
    code, j = req("POST", "/api/demo/write", {"rows": 10})
    check("回收后写入恢复原有行为", code == 200 and j.get("inserted") == 10, j)

    code, j = req("POST", "/api/slot/create")
    check("同名重建新一代", code == 200 and j.get("generation") == gen + 1, j)
    st = status()
    check("新代不继承旧代阈值",
          st["admission"]["applicable"] is False
          and st["admission"]["threshold_bytes"] is None, st["admission"])
    check("新代不继承阻断结论", st["admission"]["allowed"] is True, st["admission"])
    code, j = req("POST", "/api/demo/write", {"rows": 10})
    check("新代未设阈值: 写入放行", code == 200 and j.get("inserted") == 10, j)

    print("  … 等待租约过期(6s)")
    time.sleep(6)
    code, j = req("POST", "/api/slot/reclaim")
    check("清理回收(终态无槽)", code == 200 and j.get("reclaimed"), j)
    code, j = req("POST", "/api/slot/threshold", {"threshold_bytes": 1024})
    check("回收 tombstone 上设置阈值被拒",
          code == 409 and j.get("reason") == "lease_reclaimed", j)
    psql("DELETE FROM cdc_slot_lease")  # 彻底删掉租约行
    code, j = req("POST", "/api/slot/threshold", {"threshold_bytes": 1024})
    check("无租约时设置阈值被拒", code == 409 and j.get("reason") == "no_lease", j)

    # ---------- 汇总 ----------
    st = status()
    check("终态: 槽不存在", st["slot"]["exists"] is False, st)
    print(f"\n== 结果: {PASS} 通过, {FAIL} 失败 ==")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
