#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bench_barcode.py — exhaustive latency & scaling harness for barcode.py
======================================================================

WHAT THIS DOES
    Imports your real barcode.py as a module and drives its REAL code paths
    (Store, DurableQueue, VirtualBoard, App._scan_impl, Flusher, Poller)
    against synthetic data, measuring every latency that matters.

SAFETY — this harness CANNOT touch your production data:
    * Google Sheets  : the real SheetClient class is replaced with a fake
                       before anything can construct it. Zero network calls.
                       Verified by a guard that raises if the real one is hit.
    * scan_queue.db  : SCANNER_DB is pointed at a throwaway temp dir.
    * id_registry    : barcode.REGISTRY_FILE is repointed at a temp file.
    * scanner.log    : SCANNER_LOG repointed at the temp dir.
    * emergency log  : SCANNER_EMERGENCY repointed at the temp dir.
    Your real files' size+mtime are fingerprinted before and after and the
    harness FAILS LOUDLY if anything moved.

CLEANUP
    Everything is created inside one temp directory which is deleted in a
    finally: block — even on crash or Ctrl-C. The exact paths are printed.

USAGE
    python3 bench_barcode.py                      # full run
    python3 bench_barcode.py --quick              # ~60 s smoke run
    python3 bench_barcode.py --no-ui              # skip Tk phases
    python3 bench_barcode.py --barcode ~/barcode.py
    python3 bench_barcode.py --report bench.txt

Runtime: roughly 3-6 minutes for a full run, under 90 s with --quick.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import shutil
import sqlite3
import statistics
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

# --------------------------------------------------------------------------- #
#  0. CLI + temp sandbox. Env vars MUST be set before barcode.py is imported,
#     because DB_FILE / LOG_FILE / EMERGENCY_FILE are baked into default
#     arguments at class-definition time.
# --------------------------------------------------------------------------- #
P = argparse.ArgumentParser(description="Benchmark barcode.py end to end.")
P.add_argument("--barcode", default=None,
               help="path to barcode.py (default: auto-detect)")
P.add_argument("--quick", action="store_true",
               help="fewer iterations / smaller sizes (~60 s)")
P.add_argument("--no-ui", action="store_true",
               help="skip all Tkinter phases (headless / no display)")
P.add_argument("--report", default=None,
               help="also write the report to this file")
P.add_argument("--keep", action="store_true",
               help="do NOT delete the temp sandbox (debugging only)")
ARGS = P.parse_args()

QUICK = ARGS.quick

SANDBOX = tempfile.mkdtemp(prefix="ismun_bench_")
os.environ["SCANNER_DB"] = os.path.join(SANDBOX, "bench_scan_queue.db")
os.environ["SCANNER_LOG"] = os.path.join(SANDBOX, "bench_scanner.log")
os.environ["SCANNER_EMERGENCY"] = os.path.join(SANDBOX, "bench_emergency.jsonl")
os.environ["SCANNER_ID"] = "BENCH-STATION"
os.environ["SCANNER_SA"] = os.path.join(SANDBOX, "nonexistent-sa.json")
os.environ.pop("SCANNER_PERF", None)          # keep their perf logger quiet

BENCH_REGISTRY = os.path.join(SANDBOX, "bench_id_registry.json")

_OUT: list[str] = []


def say(line: str = "") -> None:
    print(line, flush=True)
    _OUT.append(line)


def rule(title: str = "") -> None:
    if title:
        say("\n" + "=" * 78)
        say(f"  {title}")
        say("=" * 78)
    else:
        say("-" * 78)


# --------------------------------------------------------------------------- #
#  1. Locate + fingerprint the user's real files (must be untouched)
# --------------------------------------------------------------------------- #
def find_barcode() -> str:
    if ARGS.barcode:
        p = os.path.expanduser(ARGS.barcode)
        if not os.path.exists(p):
            sys.exit(f"FATAL: --barcode path not found: {p}")
        return p
    for cand in (Path.cwd() / "barcode.py",
                 Path.home() / "barcode.py",
                 Path.home() / "Downloads" / "barcode.py",
                 Path.home() / "Downloads" / "ISMUN_BARCODES" / "barcode.py"):
        if cand.exists():
            return str(cand)
    sys.exit("FATAL: could not find barcode.py — pass --barcode /path/to/it")


BARCODE_PATH = find_barcode()


def fingerprint(paths: list[str]) -> dict[str, tuple[int, float] | None]:
    out = {}
    for p in paths:
        try:
            st = os.stat(p)
            out[p] = (st.st_size, st.st_mtime)
        except OSError:
            out[p] = None
    return out


# Real files we must never modify.
REAL_HOME = Path.home()
PROTECTED = [
    BARCODE_PATH,
    str(REAL_HOME / "Downloads" / "ISMUN_BARCODES" / "id_registry.json"),
    str(Path.cwd() / "scan_queue.db"),
    str(Path.cwd() / "scanner.log"),
    str(Path.cwd() / "scan_emergency.jsonl"),
    str(REAL_HOME / "scan_queue.db"),
    str(REAL_HOME / "scanner.log"),
    str(REAL_HOME / "scan_emergency.jsonl"),
]
FP_BEFORE = fingerprint(PROTECTED)


# --------------------------------------------------------------------------- #
#  2. Import barcode.py under the sandboxed env
# --------------------------------------------------------------------------- #
import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location("barcode_under_test",
                                               BARCODE_PATH)
if _spec is None or _spec.loader is None:
    sys.exit(f"FATAL: cannot import {BARCODE_PATH}")
bc = importlib.util.module_from_spec(_spec)
sys.modules["barcode_under_test"] = bc
try:
    _spec.loader.exec_module(bc)
except Exception as e:                                    # noqa: BLE001
    shutil.rmtree(SANDBOX, ignore_errors=True)
    sys.exit(f"FATAL: importing barcode.py raised {e!r}\n"
             "  (missing deps? try: pip install google-auth "
             "google-api-python-client)")

# Repoint the registry constant (read at call time by load_registry()).
bc.REGISTRY_FILE = BENCH_REGISTRY


# --------------------------------------------------------------------------- #
#  3. Hard network lockout — the real SheetClient must never be constructed
# --------------------------------------------------------------------------- #
class _ForbiddenSheetClient:
    def __init__(self, *a, **k):
        raise AssertionError(
            "SAFETY VIOLATION: the real SheetClient was constructed during "
            "the benchmark. Aborting before any network call.")


_REAL_SHEETCLIENT = bc.SheetClient
bc.SheetClient = _ForbiddenSheetClient


class FakeSheet:
    """Stand-in for SheetClient. Implements the full pipeline API.
    Optional simulated network latency and 429 injection."""

    def __init__(self, latency_s: float = 0.0, fail_every: int = 0):
        self.rows: list[list[str]] = []
        self.roster: list[list[str]] = []
        self.latency = latency_s
        self.fail_every = fail_every
        self.appends = 0
        self.reads = 0
        self.roster_pushes = 0
        self.heals = 0
        self.bytes_read = 0
        self._lock = threading.Lock()

    def _net(self):
        if self.latency:
            time.sleep(self.latency)

    def append_log_rows(self, values):
        self._net()
        with self._lock:
            self.appends += 1
            if self.fail_every and self.appends % self.fail_every == 0:
                raise RuntimeError("simulated 429 / transport error")
            self.rows.extend([list(map(str, v)) for v in values])

    def read_log_all(self):
        self._net()
        with self._lock:
            self.reads += 1
            snap = [list(r) for r in self.rows]
        self.bytes_read += sum(len(c) for r in snap for c in r)
        return snap

    def push_roster(self, rows):
        self._net()
        self.roster_pushes += 1
        self.roster = [list(r) for r in rows]

    def re_setup(self):
        self._net()

    def verify_and_heal(self):
        self._net()
        self.heals += 1
        return False


# Silence modal dialogs so an ALREADY scan cannot block the harness.
class _NoModal:
    @staticmethod
    def showwarning(*a, **k):
        return "ok"

    @staticmethod
    def showerror(*a, **k):
        return "ok"

    @staticmethod
    def showinfo(*a, **k):
        return "ok"

    @staticmethod
    def askokcancel(*a, **k):
        return True


bc.messagebox = _NoModal


# --------------------------------------------------------------------------- #
#  4. Instrumented lock — measures contention inside Store
# --------------------------------------------------------------------------- #
class InstrumentedRLock:
    """Drop-in for threading.RLock that records acquire-wait and hold times.
    Reentrancy-aware: only the outermost acquire/release pair is timed."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tl = threading.local()
        self.waits_ns: list[int] = []
        self.holds_ns: list[int] = []
        self.enabled = False
        self._guard = threading.Lock()

    def acquire(self, blocking=True, timeout=-1):
        depth = getattr(self._tl, "depth", 0)
        if depth == 0 and self.enabled:
            t0 = time.perf_counter_ns()
            got = self._lock.acquire(blocking, timeout)
            t1 = time.perf_counter_ns()
            if got:
                with self._guard:
                    self.waits_ns.append(t1 - t0)
                self._tl.t_in = t1
        else:
            got = self._lock.acquire(blocking, timeout)
        if got:
            self._tl.depth = depth + 1
        return got

    def release(self):
        depth = getattr(self._tl, "depth", 1)
        self._tl.depth = depth - 1
        if depth - 1 == 0 and self.enabled:
            t_in = getattr(self._tl, "t_in", None)
            if t_in is not None:
                with self._guard:
                    self.holds_ns.append(time.perf_counter_ns() - t_in)
                self._tl.t_in = None
        self._lock.release()

    __enter__ = acquire

    def __exit__(self, *exc):
        self.release()
        return False

    def reset(self):
        with self._guard:
            self.waits_ns.clear()
            self.holds_ns.clear()


# --------------------------------------------------------------------------- #
#  5. Stats helpers
# --------------------------------------------------------------------------- #
def pct(xs: list[float], q: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    k = (len(s) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


class Stat:
    def __init__(self, name: str, samples_ms: list[float], unit: str = "ms"):
        self.name = name
        self.s = samples_ms
        self.unit = unit

    @property
    def n(self):
        return len(self.s)

    def row(self) -> str:
        if not self.s:
            return f"{self.name:<44} {'(no samples)':>30}"
        return (f"{self.name:<44} n={self.n:<6} "
                f"p50={pct(self.s, .50):8.3f} p90={pct(self.s, .90):8.3f} "
                f"p99={pct(self.s, .99):8.3f} max={max(self.s):8.3f}")


HDR = (f"{'measurement':<44} {'count':<8} "
       f"{'p50 ms':>8} {'p90 ms':>8} {'p99 ms':>8} {'max ms':>8}")


def table(stats: list[Stat]) -> None:
    say(HDR)
    say("-" * 92)
    for st in stats:
        say(st.row())


VERDICTS: list[tuple[str, str, str]] = []      # (level, area, message)


def verdict(level: str, area: str, msg: str) -> None:
    VERDICTS.append((level, area, msg))


# --------------------------------------------------------------------------- #
#  6. Synthetic data builders
# --------------------------------------------------------------------------- #
COMMITTEES = ["UNSC", "UNHRC", "DISEC", "ECOSOC", "WHO", "UNEP", "AIPPM"]
OC = ["CAMP", "DESIGN", "LOGISTICS", "MARKETING", "PHOTOGRAPHY", "PRESS",
      "TECH"]


def make_registry(n: int) -> dict:
    reg = {}
    for i in range(1, n + 1):
        pid = f"ISMUN-{i:04d}"
        if i % 25 == 0:
            reg[pid] = {"name": f"OC Member {i}",
                        "committee": OC[i % len(OC)], "group": "Member"}
        elif i % 97 == 0:
            reg[pid] = {"name": f"Sec Gen {i}", "committee": "SEC",
                        "group": "SEC"}
        else:
            reg[pid] = {"name": f"Delegate Number {i}",
                        "committee": COMMITTEES[i % len(COMMITTEES)],
                        "group": f"Portfolio {i % 40}"}
    return reg


def write_registry(n: int) -> dict:
    reg = make_registry(n)
    with open(BENCH_REGISTRY, "w", encoding="utf-8") as f:
        json.dump(reg, f)
    return reg


def make_sheet_rows(n_people: int, n_rows: int) -> list[list[str]]:
    """Realistic Scan Log rows: [ts, pid, name, meal, status, scanid, mach]."""
    base = datetime(2026, 8, 1, 8, 0, 0)
    rows = []
    meals = bc.MEALS
    for i in range(n_rows):
        pid = f"ISMUN-{(i % n_people) + 1:04d}"
        meal = meals[(i // max(1, n_people)) % len(meals)]
        ts = (base + timedelta(seconds=i * 3)).strftime(bc.TS_FMT)
        rows.append([ts, pid, f"Delegate Number {(i % n_people) + 1}", meal,
                     "COLLECTED", f"BENCH-{i:08d}", "BENCH-STATION"])
    return rows


def fresh_store(n_people: int, db_path: str | None = None):
    """A brand-new Store on a brand-new DB, with an instrumented lock."""
    write_registry(n_people)
    path = db_path or os.path.join(SANDBOX, f"db_{n_people}_{time.time_ns()}.db")
    dq = bc.DurableQueue(path)
    store = bc.Store(dq)
    store.lock = InstrumentedRLock()
    return store, dq, path


# --------------------------------------------------------------------------- #
#  PHASE 1 — micro-benchmarks on the hot scan path
# --------------------------------------------------------------------------- #
def phase1_micro() -> None:
    rule("PHASE 1 — hot-path micro-benchmarks (no UI, no network)")
    say("Every operation the scan handler performs, timed in isolation.\n")

    N_PEOPLE = 200 if QUICK else 1000
    ITER = 2000 if QUICK else 8000
    PERSIST_ITER = 400 if QUICK else 1500

    store, dq, _ = fresh_store(N_PEOPLE)
    pids = store.roster_ids()

    # --- resolve(): exact id hit ---
    s_exact = []
    for i in range(ITER):
        code = pids[i % len(pids)]
        t = time.perf_counter_ns()
        store.resolve(code)
        s_exact.append((time.perf_counter_ns() - t) / 1e6)

    # --- resolve(): numeric-suffix hit (what the scanner gun sends) ---
    s_num = []
    for i in range(ITER):
        code = str((i % N_PEOPLE) + 1)
        t = time.perf_counter_ns()
        store.resolve(code)
        s_num.append((time.perf_counter_ns() - t) / 1e6)

    # --- resolve(): miss (unknown barcode -> rejection path) ---
    s_miss = []
    for i in range(ITER):
        t = time.perf_counter_ns()
        store.resolve(f"GARBAGE-{i}")
        s_miss.append((time.perf_counter_ns() - t) / 1e6)

    # --- person_fields() ---
    s_pf = []
    for i in range(ITER):
        pid = pids[i % len(pids)]
        t = time.perf_counter_ns()
        store.person_fields(pid)
        s_pf.append((time.perf_counter_ns() - t) / 1e6)

    # --- scan_decide(): the in-memory decision, COLLECTED path ---
    s_dec_new, s_dec_dup = [], []
    combos = [(p, m) for m in bc.MEALS for p in pids]
    for i, (pid, meal) in enumerate(combos[:ITER]):
        t = time.perf_counter_ns()
        store.scan_decide(pid, meal)
        s_dec_new.append((time.perf_counter_ns() - t) / 1e6)

    # --- scan_decide(): the ALREADY path (repeat scan) ---
    for i, (pid, meal) in enumerate(combos[:min(ITER, 2000)]):
        t = time.perf_counter_ns()
        store.scan_decide(pid, meal)
        s_dec_dup.append((time.perf_counter_ns() - t) / 1e6)

    # --- scan_persist(): SQLite INSERT + commit with synchronous=FULL ---
    s_persist = []
    for i in range(PERSIST_ITER):
        rec = {"scan_id": f"P-{i:09d}", "ts": datetime.now().strftime(bc.TS_FMT),
               "pid": pids[i % len(pids)], "name": "X",
               "meal": bc.MEALS[i % len(bc.MEALS)], "status": "COLLECTED",
               "machine": "BENCH-STATION"}
        t = time.perf_counter_ns()
        store.scan_persist(rec)
        s_persist.append((time.perf_counter_ns() - t) / 1e6)

    # --- unsynced_count(): called on EVERY scan for the status badge ---
    s_cnt = []
    for _ in range(min(ITER, 3000)):
        t = time.perf_counter_ns()
        store.unsynced_count()
        s_cnt.append((time.perf_counter_ns() - t) / 1e6)

    table([
        Stat("resolve() exact id hit", s_exact),
        Stat("resolve() numeric-suffix hit", s_num),
        Stat("resolve() miss (rejected barcode)", s_miss),
        Stat("person_fields()", s_pf),
        Stat("scan_decide() new -> COLLECTED", s_dec_new),
        Stat("scan_decide() repeat -> ALREADY", s_dec_dup),
        Stat("scan_persist() SQLite fsync commit", s_persist),
        Stat("unsynced_count() (per-scan badge)", s_cnt),
    ])

    p99_persist = pct(s_persist, .99)
    say(f"\nSQLite is configured synchronous=FULL, so every scan costs a real "
        f"fsync.\nMeasured p99 = {p99_persist:.2f} ms on THIS disk. This is "
        f"off the perceived\npath (it runs after update_idletasks), so it "
        f"caps sustained throughput,\nnot felt latency: ceiling is "
        f"~{1000.0 / max(p99_persist, 1e-6):.0f} scans/sec per station.")

    if p99_persist > 50:
        verdict("WARN", "durability",
                f"fsync p99 {p99_persist:.1f} ms is slow for this disk — "
                "sustained scan rate is capped near "
                f"{1000 / p99_persist:.0f}/s.")
    else:
        verdict("PASS", "durability",
                f"fsync p99 {p99_persist:.1f} ms — durability cost is fine.")

    if pct(s_dec_new, .99) > 1.0:
        verdict("WARN", "scan-decide",
                "in-memory decide p99 exceeded 1 ms.")
    else:
        verdict("PASS", "scan-decide",
                f"in-memory decide p99 {pct(s_dec_new, .99):.3f} ms.")

    dq._drop_thread_conn()


# --------------------------------------------------------------------------- #
#  PHASE 2 — local queue scaling as the DB grows over a multi-day event
# --------------------------------------------------------------------------- #
def phase2_queue_scaling() -> None:
    rule("PHASE 2 — SQLite queue cost as the local DB grows")
    say("Does a big scan_queue.db slow anything down mid-event?\n")

    sizes = [1_000, 5_000] if QUICK else [1_000, 10_000, 50_000, 150_000]
    rows_hdr = (f"{'db rows':>10} {'pending(100)':>14} {'pending(ALL)':>14} "
                f"{'pending_count':>14} {'all_rows()':>12} {'file MB':>9}")
    say(rows_hdr)
    say("-" * len(rows_hdr))

    warned = False
    for n in sizes:
        path = os.path.join(SANDBOX, f"scale_{n}.db")
        dq = bc.DurableQueue(path)
        # Bulk-load fast on a private connection (setup, not measurement).
        con = sqlite3.connect(path)
        con.execute("PRAGMA synchronous=OFF")
        con.executemany(
            "INSERT INTO scans(scan_id, ts, pid, name, meal, status, machine,"
            " synced) VALUES (?,?,?,?,?,?,?,?)",
            [(f"S{n}-{i:09d}", datetime(2026, 8, 1).strftime(bc.TS_FMT),
              f"ISMUN-{(i % 1000) + 1:04d}", "Name", bc.MEALS[i % 12],
              "COLLECTED", "BENCH", 1 if i < n - 250 else 0)
             for i in range(n)])
        con.commit()
        con.close()

        def med(fn, reps=7):
            xs = []
            for _ in range(reps):
                t = time.perf_counter_ns()
                fn()
                xs.append((time.perf_counter_ns() - t) / 1e6)
            return statistics.median(xs)

        t_p100 = med(lambda: dq.pending(100))
        t_pall = med(lambda: dq.pending(1_000_000_000))
        t_cnt = med(lambda: dq.pending_count())
        t_all = med(lambda: dq.all_rows(), reps=3)
        mb = os.path.getsize(path) / 1e6

        say(f"{n:>10,} {t_p100:>13.3f} {t_pall:>13.3f} {t_cnt:>13.3f} "
            f"{t_all:>11.3f} {mb:>8.2f}")

        if t_cnt > 5.0 and not warned:
            warned = True
        dq._drop_thread_conn()

    say("\nNotes:")
    say("  pending(100)   — the flusher's batch read. Indexed, stays flat.")
    say("  pending(ALL)   — called by reconcile() every 5 s with a limit of")
    say("                   1,000,000,000. Only UNSYNCED rows match, so it")
    say("                   stays small in practice; it only blows up if the")
    say("                   network is down for a long stretch.")
    say("  pending_count()— runs on EVERY scan for the status badge. It is a")
    say("                   COUNT(*) over the partial index, so it is O(1)-ish")
    say("                   in unsynced rows, not total rows.")
    say("  all_rows()     — startup only (bootstrap_local). Grows linearly;")
    say("                   this is your app-launch cost, not a scan cost.")

    if warned:
        verdict("WARN", "queue",
                "pending_count() exceeded 5 ms at scale — it runs on every "
                "scan.")
    else:
        verdict("PASS", "queue",
                "local queue reads stay flat as the DB grows.")


# --------------------------------------------------------------------------- #
#  PHASE 3 — reconcile() scaling: the sheet-as-truth rebuild
# --------------------------------------------------------------------------- #
def phase3_reconcile() -> None:
    rule("PHASE 3 — reconcile() vs Scan Log size  [the main scaling risk]")
    say("The poller does a FULL sheet read + full state rebuild every 5 s.")
    say("Cost grows linearly with total rows ever logged. Measured below,")
    say("with the lock-hold time isolated (that is what can block a scan).\n")

    people = 400 if QUICK else 1200
    sizes = ([500, 2_000, 5_000] if QUICK
             else [500, 2_000, 5_000, 10_000, 20_000, 40_000])
    reps = 3 if QUICK else 6

    store, dq, _ = fresh_store(people)
    lk: InstrumentedRLock = store.lock

    hdr = (f"{'sheet rows':>11} {'reconcile p50':>14} {'p99':>9} "
           f"{'lock hold p99':>14} {'changed cells':>14} {'payload KB':>11}")
    say(hdr)
    say("-" * len(hdr))

    worst_lock = 0.0
    worst_total = 0.0
    worst_payload = 0.0
    for n in sizes:
        rows = make_sheet_rows(people, n)
        payload_kb = sum(len(c) for r in rows for c in r) / 1024.0
        # Prime once so we measure steady-state (no first-run cold cost).
        store.reconcile(rows)
        lk.reset()
        lk.enabled = True
        durations, changed_n = [], 0
        for _ in range(reps):
            t = time.perf_counter_ns()
            changed = store.reconcile(rows)
            durations.append((time.perf_counter_ns() - t) / 1e6)
            changed_n = len(changed)
        lk.enabled = False
        hold_p99 = pct([h / 1e6 for h in lk.holds_ns], .99)
        worst_lock = max(worst_lock, hold_p99)
        worst_total = max(worst_total, pct(durations, .99))
        worst_payload = max(worst_payload, payload_kb)
        say(f"{n:>11,} {pct(durations, .5):>13.2f} {pct(durations, .99):>8.2f} "
            f"{hold_p99:>13.2f} {changed_n:>14,} {payload_kb:>10.1f}")
        del rows
        gc.collect()

    say("\nInterpretation:")
    say("  * reconcile p99 is the poller thread's work — it does NOT block")
    say("    the UI except for the portion spent holding store.lock.")
    say("  * 'lock hold p99' IS the blocking window. A scan that lands inside")
    say("    it waits that long before scan_decide() can proceed.")
    say("  * 'payload KB' is what read_log_all() pulls from Google every 5 s.")

    if worst_lock > 16.0:
        verdict("WARN", "reconcile",
                f"reconcile holds store.lock up to {worst_lock:.1f} ms at the "
                "largest tested log size — a scan landing in that window "
                "stalls by that much.")
    else:
        verdict("PASS", "reconcile",
                f"reconcile lock-hold p99 stays at {worst_lock:.1f} ms — "
                "below one 60 Hz frame.")

    if worst_total > 1000:
        verdict("WARN", "reconcile",
                f"reconcile total p99 {worst_total:.0f} ms approaches the 5 s "
                "poll interval.")

    say(f"\nGoogle recommends keeping a request payload under 2 MB. The")
    say(f"largest log size tested here ({sizes[-1]:,} rows) pulls "
        f"{worst_payload / 1024:.2f} MB")
    say("EVERY 5 SECONDS, on every station, forever.")
    if worst_payload / 1024 > 2.0:
        verdict("WARN", "payload",
                f"at {sizes[-1]:,} logged rows the 5 s poll pulls "
                f"{worst_payload / 1024:.1f} MB — past Google's 2 MB "
                "guidance. Estimate your real row count before the event.")
    else:
        verdict("PASS", "payload",
                f"largest tested poll payload {worst_payload / 1024:.2f} MB — "
                "inside Google's 2 MB guidance.")

    dq._drop_thread_conn()


# --------------------------------------------------------------------------- #
#  PHASE 4 — concurrency: does a background poll stall a scan?
# --------------------------------------------------------------------------- #
def phase4_contention() -> None:
    rule("PHASE 4 — lock contention: scanning WHILE the poller reconciles")
    say("This is the realistic worst case — a delegate scans at the exact")
    say("moment the 5 s poll rebuilds state from a large sheet.\n")

    people = 400 if QUICK else 1200
    sheet_n = 5_000 if QUICK else 25_000
    scans = 300 if QUICK else 900

    store, dq, _ = fresh_store(people)
    pids = store.roster_ids()
    rows = make_sheet_rows(people, sheet_n)
    store.reconcile(rows)

    stop = threading.Event()
    recon_count = [0]

    def poller_loop():
        while not stop.is_set():
            store.reconcile(rows)
            recon_count[0] += 1
            time.sleep(0.01)

    # Baseline: no background reconcile.
    base = []
    for i in range(scans):
        pid, meal = pids[i % len(pids)], bc.MEALS[(i // len(pids)) % 12]
        t = time.perf_counter_ns()
        store.scan_decide(pid, meal)
        base.append((time.perf_counter_ns() - t) / 1e6)

    # Contended: reconcile hammering in the background.
    th = threading.Thread(target=poller_loop, daemon=True)
    th.start()
    time.sleep(0.2)
    cont = []
    for i in range(scans):
        pid, meal = pids[i % len(pids)], bc.MEALS[(i // len(pids)) % 12]
        t = time.perf_counter_ns()
        store.scan_decide(pid, meal)
        cont.append((time.perf_counter_ns() - t) / 1e6)
    stop.set()
    th.join(timeout=5)

    table([
        Stat(f"scan_decide() idle (log={sheet_n:,})", base),
        Stat(f"scan_decide() DURING reconcile", cont),
    ])
    say(f"\nBackground reconciles completed during the test: {recon_count[0]}")
    say("(Real life runs ONE reconcile every 5 s, so this is far harsher "
        "than production.)")

    delta = pct(cont, .99) - pct(base, .99)
    say(f"p99 penalty from contention: {delta:+.3f} ms")
    if pct(cont, .999 if len(cont) > 1000 else .99) > 50:
        verdict("WARN", "contention",
                f"worst-case scan under continuous reconcile hit "
                f"{max(cont):.1f} ms.")
    else:
        verdict("PASS", "contention",
                f"scan stayed under {max(cont):.1f} ms even with a reconcile "
                "storm.")

    del rows
    gc.collect()
    dq._drop_thread_conn()


# --------------------------------------------------------------------------- #
#  PHASE 5 — Tk: VirtualBoard rendering vs roster size
# --------------------------------------------------------------------------- #
def _kill_app(app) -> int:
    """Cancel every pending 'after' timer, then destroy the root.

    barcode.py schedules an after(900) flash-decay per scan plus a repeating
    after(100) _drain_ui. Destroying the root without cancelling them leaves
    orphaned Tcl callbacks that fire against the NEXT root and spam
    'invalid command name'. Returns how many timers were outstanding — a
    useful number in its own right (see PHASE 10)."""
    n = 0
    try:
        pending = app.tk.call("after", "info")
        ids = app.tk.splitlist(pending) if isinstance(pending, str) else pending
        n = len(ids)
        for aid in ids:
            try:
                app.after_cancel(aid)
            except Exception:                             # noqa: BLE001
                pass
    except Exception:                                     # noqa: BLE001
        pass
    try:
        _kill_app(app)
    except Exception:                                     # noqa: BLE001
        pass
    return n


def _make_app(n_people: int):
    """Build a real App on a real Tk root, mapped and sized."""
    store, dq, path = fresh_store(n_people)
    app = bc.App(store)
    app.geometry("1500x800")
    app.update_idletasks()
    app.update()
    bv = app.boardview
    # Force slot allocation if <Configure> has not delivered a real height.
    if bv._nslots == 0 or bv.body.winfo_height() <= 1:
        bv.body.configure(height=700)
        app.update_idletasks()
        bv._on_resize()
        app.update()
    return app, store, dq, path


def phase5_board(ui_ok: bool) -> None:
    rule("PHASE 5 — VirtualBoard render cost vs roster size (Tk)")
    if not ui_ok:
        say("SKIPPED — no usable display.")
        return
    say("The board is virtualized, so cost should be FLAT in roster size.")
    say("If these numbers grow with N, virtualization is broken.\n")

    sizes = [100, 800] if QUICK else [100, 500, 2_000, 5_000, 10_000]
    reps = 40 if QUICK else 150

    hdr = (f"{'roster':>8} {'slots':>6} {'render() p50':>13} {'p99':>8} "
           f"{'render_pid p50':>15} {'p99':>8} {'set_order p50':>14} "
           f"{'scroll p99':>11}")
    say(hdr)
    say("-" * len(hdr))

    growth = []
    for n in sizes:
        app, store, dq, _ = _make_app(n)
        bv = app.boardview
        pids = store.roster_ids()
        # Populate ~40% of cells so rendering is not trivially empty.
        now = datetime.now()
        with store.lock:
            for i, p in enumerate(pids):
                slot = store.state.setdefault(p, {})
                for m in bc.MEALS[: (i % 12)]:
                    slot[m] = now
        bv.set_order(store.known_pids())
        app.update()

        r_full, r_one, r_set, r_scroll = [], [], [], []
        for i in range(reps):
            bv._cache = [[None] * len(bc.COLUMNS) for _ in range(bv._nslots)]
            t = time.perf_counter_ns()
            bv.render()
            r_full.append((time.perf_counter_ns() - t) / 1e6)

            pid = pids[i % len(pids)]
            bv.ensure_pid(pid)
            t = time.perf_counter_ns()
            bv.render_pid(pid)
            r_one.append((time.perf_counter_ns() - t) / 1e6)

            t = time.perf_counter_ns()
            bv._wheel_steps(3 if i % 2 else -3)
            r_scroll.append((time.perf_counter_ns() - t) / 1e6)

        for _ in range(max(5, reps // 10)):
            t = time.perf_counter_ns()
            bv.set_order(store.known_pids())
            r_set.append((time.perf_counter_ns() - t) / 1e6)

        say(f"{n:>8,} {bv._nslots:>6} {pct(r_full, .5):>12.3f} "
            f"{pct(r_full, .99):>7.3f} {pct(r_one, .5):>14.3f} "
            f"{pct(r_one, .99):>7.3f} {pct(r_set, .5):>13.3f} "
            f"{pct(r_scroll, .99):>10.3f}")
        growth.append((n, pct(r_full, .5), pct(r_set, .5)))

        _kill_app(app)
        dq._drop_thread_conn()
        gc.collect()

    if len(growth) >= 2:
        n0, r0, s0 = growth[0]
        n1, r1, s1 = growth[-1]
        ratio_n = n1 / n0
        ratio_r = (r1 / r0) if r0 > 0 else 1.0
        say(f"\nRoster grew {ratio_n:.0f}x; full render() cost changed "
            f"{ratio_r:.2f}x.")
        if ratio_r < 2.0:
            verdict("PASS", "board",
                    f"render() is flat in roster size ({ratio_n:.0f}x people "
                    f"-> {ratio_r:.2f}x cost). Virtualization works.")
        else:
            verdict("WARN", "board",
                    f"render() grew {ratio_r:.2f}x for {ratio_n:.0f}x people "
                    "— virtualization may be leaking.")
        say(f"set_order() went {s0:.2f} ms -> {s1:.2f} ms (this one IS O(N):")
        say("it rebuilds the pid->index map; it fires on registry reload only).")
        if s1 > 100:
            verdict("WARN", "board",
                    f"set_order() takes {s1:.0f} ms at {n1:,} people — a "
                    "registry reload will visibly hitch the UI.")


# --------------------------------------------------------------------------- #
#  PHASE 6 — Tk: true end-to-end perceived scan latency
# --------------------------------------------------------------------------- #
def phase6_perceived(ui_ok: bool) -> None:
    rule("PHASE 6 — END-TO-END perceived scan latency (real App._scan_impl)")
    if not ui_ok:
        say("SKIPPED — no usable display.")
        return
    say("Drives the real Entry widget through the real handler.")
    say("  PERCEIVED = keypress -> update_idletasks() returns (pixels on")
    say("              screen). This is what the operator feels.")
    say("  TOTAL     = the whole handler, including fsync + badge + modal.")
    say("              This is what caps how fast you can scan back-to-back.\n")

    sizes = [200, 1_500] if QUICK else [200, 1_000, 5_000, 10_000]
    scans = 200 if QUICK else 600

    hdr = (f"{'roster':>8} {'perceived p50':>14} {'p90':>8} {'p99':>8} "
           f"{'max':>8} | {'total p50':>10} {'p99':>8} {'max':>8}")
    say(hdr)
    say("-" * len(hdr))

    worst_perceived = 0.0
    worst_total = 0.0
    for n in sizes:
        app, store, dq, _ = _make_app(n)
        pids = store.roster_ids()

        marks: list[float] = []
        real_uit = app.update_idletasks

        def spy_uit():
            real_uit()
            marks.append(time.perf_counter())

        app.update_idletasks = spy_uit          # instance-level shadow

        perceived, total = [], []
        for i in range(scans):
            pid = pids[i % len(pids)]
            meal = bc.MEALS[(i // len(pids)) % len(bc.MEALS)]
            app.meal_var.set(meal)
            app.entry.delete(0, "end")
            app.entry.insert(0, pid)
            marks.clear()
            t0 = time.perf_counter()
            app._scan_impl()
            t1 = time.perf_counter()
            if marks:
                perceived.append((marks[0] - t0) * 1000.0)
            total.append((t1 - t0) * 1000.0)
            if i % 50 == 0:
                app.update()                    # let the 10 Hz pump breathe

        app.update_idletasks = real_uit
        say(f"{n:>8,} {pct(perceived, .5):>13.3f} {pct(perceived, .9):>7.3f} "
            f"{pct(perceived, .99):>7.3f} {max(perceived):>7.3f} | "
            f"{pct(total, .5):>9.3f} {pct(total, .99):>7.3f} "
            f"{max(total):>7.3f}")
        worst_perceived = max(worst_perceived, pct(perceived, .99))
        worst_total = max(worst_total, pct(total, .99))

        _kill_app(app)
        dq._drop_thread_conn()
        gc.collect()

    say("\nBudget reference: 16.7 ms = one frame at 60 Hz. Under ~50 ms a")
    say("scan feels instant to a human; over ~100 ms it feels laggy.")
    if worst_perceived < 16.7:
        verdict("PASS", "perceived",
                f"perceived p99 {worst_perceived:.1f} ms — sub-frame, "
                "instant.")
    elif worst_perceived < 50:
        verdict("PASS", "perceived",
                f"perceived p99 {worst_perceived:.1f} ms — comfortably "
                "instant.")
    else:
        verdict("WARN", "perceived",
                f"perceived p99 {worst_perceived:.1f} ms — operators may "
                "notice.")
    say(f"\nBack-to-back ceiling from TOTAL p99 "
        f"({worst_total:.1f} ms): ~{1000 / max(worst_total, 1e-6):.0f} "
        "scans/sec/station.")


# --------------------------------------------------------------------------- #
#  PHASE 7 — Tk: the "too many entries" question, directly
# --------------------------------------------------------------------------- #
def phase7_big_state(ui_ok: bool) -> None:
    rule("PHASE 7 — does the dashboard lag once the board is FULL of entries?")
    if not ui_ok:
        say("SKIPPED — no usable display.")
        return
    say("Fills state so every person has every meal collected (the state at")
    say("the END of a 3-day event), then re-measures the same operations.\n")

    n = 800 if QUICK else 5_000
    app, store, dq, _ = _make_app(n)
    bv = app.boardview
    pids = store.roster_ids()

    def bench_scan(label: str) -> list[float]:
        out = []
        for i in range(150 if QUICK else 400):
            pid = pids[i % len(pids)]
            app.entry.delete(0, "end")
            app.entry.insert(0, pid)
            app.meal_var.set(bc.MEALS[i % 12])
            t = time.perf_counter()
            app._scan_impl()
            out.append((time.perf_counter() - t) * 1000.0)
        return out

    empty = bench_scan("empty")

    now = datetime.now()
    with store.lock:
        for p in pids:
            store.state[p] = {m: now for m in bc.MEALS}
    bv.set_order(store.known_pids())
    bv._cache = [[None] * len(bc.COLUMNS) for _ in range(bv._nslots)]
    bv.render()
    app.update()

    full = bench_scan("full")

    # Rendering with a fully-green board.
    r_full = []
    for i in range(80 if QUICK else 250):
        bv._cache = [[None] * len(bc.COLUMNS) for _ in range(bv._nslots)]
        t = time.perf_counter_ns()
        bv.render()
        r_full.append((time.perf_counter_ns() - t) / 1e6)

    # Scrolling through a fully-populated board.
    r_scroll = []
    for i in range(200 if QUICK else 600):
        t = time.perf_counter_ns()
        bv._wheel_steps(3 if (i // 20) % 2 == 0 else -3)
        r_scroll.append((time.perf_counter_ns() - t) / 1e6)

    table([
        Stat(f"full scan handler, EMPTY board ({n:,} ppl)", empty),
        Stat(f"full scan handler, FULL board ({n * 12:,} cells)", full),
        Stat("render() with every cell green", r_full),
        Stat("mouse-wheel scroll step, full board", r_scroll),
    ])

    d = pct(full, .99) - pct(empty, .99)
    say(f"\nDelta p99 empty -> completely full board: {d:+.3f} ms")
    if abs(d) < 10:
        verdict("PASS", "big-state",
                f"a fully-populated board costs {d:+.1f} ms vs an empty one — "
                "no meaningful lag from data volume.")
    else:
        verdict("WARN", "big-state",
                f"a full board is {d:+.1f} ms slower per scan.")

    if pct(r_scroll, .99) > 16.7:
        verdict("WARN", "scroll",
                f"scroll step p99 {pct(r_scroll, .99):.1f} ms exceeds one "
                "frame — scrolling will feel choppy.")
    else:
        verdict("PASS", "scroll",
                f"scroll step p99 {pct(r_scroll, .99):.1f} ms — smooth.")

    _kill_app(app)
    dq._drop_thread_conn()
    gc.collect()


# --------------------------------------------------------------------------- #
#  PHASE 8 — the UI event pump under a huge reconcile diff
# --------------------------------------------------------------------------- #
def phase8_ui_pump(ui_ok: bool) -> None:
    rule("PHASE 8 — _drain_ui() with a large 'cells' payload")
    if not ui_ok:
        say("SKIPPED — no usable display.")
        return
    say("The FIRST poll after launch marks every already-collected cell as")
    say("'changed', so the payload can be enormous. Measured here.\n")

    n = 800 if QUICK else 5_000
    app, store, dq, _ = _make_app(n)
    pids = store.roster_ids()

    hdr = f"{'changed cells':>15} {'_drain_ui() ms':>16}"
    say(hdr)
    say("-" * len(hdr))
    worst = 0.0
    for frac in ([0.1, 1.0] if QUICK else [0.01, 0.1, 0.5, 1.0]):
        changed = {(p, m) for p in pids[: max(1, int(len(pids) * frac))]
                   for m in bc.MEALS}
        app.ui_events.put(("cells", changed))
        t = time.perf_counter_ns()
        app._drain_ui()
        dt = (time.perf_counter_ns() - t) / 1e6
        worst = max(worst, dt)
        say(f"{len(changed):>15,} {dt:>15.2f}")

    say("\n_drain_ui() runs ON THE UI THREAD. Whatever it costs is a freeze.")
    if worst > 100:
        verdict("WARN", "ui-pump",
                f"the first-poll 'cells' payload froze the UI for "
                f"{worst:.0f} ms at {n:,} people.")
    else:
        verdict("PASS", "ui-pump",
                f"largest 'cells' payload handled in {worst:.0f} ms.")

    _kill_app(app)
    dq._drop_thread_conn()
    gc.collect()


# --------------------------------------------------------------------------- #
#  PHASE 9 — full pipeline with a fake Sheets backend (Flusher + Poller)
# --------------------------------------------------------------------------- #
def phase9_pipeline() -> None:
    rule("PHASE 9 — live pipeline: Flusher + Poller against a fake Sheets API")
    say("Real threads, real batching, real pacing — fake network. Confirms")
    say("scans reach the 'sheet' and measures the write/read call budget.\n")

    import queue as _q
    people = 300 if QUICK else 800
    n_scans = 250 if QUICK else 700
    run_s = 20.0 if QUICK else 40.0

    store, dq, _ = fresh_store(people)
    pids = store.roster_ids()
    ui: _q.Queue = _q.Queue()

    fake = FakeSheet(latency_s=0.05)           # 50 ms simulated round trip
    mgr = bc.SheetClientManager(ui, factory=lambda: fake)
    mgr.start()
    flusher = bc.Flusher(store, mgr, ui)
    poller = bc.Poller(store, mgr, ui)
    flusher.start()
    poller.start()

    t_start = time.perf_counter()
    for i in range(n_scans):
        pid = pids[i % len(pids)]
        meal = bc.MEALS[(i // len(pids)) % len(bc.MEALS)]
        res = store.scan_decide(pid, meal)
        store.scan_persist(res["rec"])
        store.flush_event.set()
        time.sleep(0.004)                       # ~250 scans/s offered load
    t_offered = time.perf_counter() - t_start

    deadline = time.perf_counter() + run_s
    while time.perf_counter() < deadline:
        if store.unsynced_count() == 0 and fake.appends > 0:
            break
        time.sleep(0.25)
    elapsed = time.perf_counter() - t_start

    say(f"scans offered            : {n_scans} in {t_offered:.2f} s")
    say(f"time to fully sync       : {elapsed:.2f} s")
    say(f"rows landed in sheet     : {len(fake.rows)}")
    say(f"unsynced left            : {store.unsynced_count()}")
    say(f"append (write) calls     : {fake.appends}")
    say(f"read_log_all calls       : {fake.reads}")
    say(f"push_roster calls        : {fake.roster_pushes}")
    if fake.appends:
        say(f"rows per append          : {len(fake.rows) / fake.appends:.1f} "
            f"(cap is MAX_BATCH={bc.MAX_BATCH})")
    if elapsed > 0:
        say(f"observed write rate      : {fake.appends / elapsed * 60:.1f} "
            "calls/min  (Google cap: 60/min/user, shared by ALL stations)")
        say(f"observed read rate       : {fake.reads / elapsed * 60:.1f} "
            "calls/min  (Google cap: 60/min/user)")

    lost = n_scans - len(fake.rows)
    if lost == 0:
        verdict("PASS", "pipeline",
                f"all {n_scans} scans reached the sheet, zero loss.")
    elif store.unsynced_count() > 0:
        verdict("PASS", "pipeline",
                f"{lost} scan(s) still queued locally at cutoff — safe, just "
                "not yet flushed.")
    else:
        verdict("FAIL", "pipeline",
                f"{lost} scan(s) vanished — neither in the sheet nor queued.")

    wr = fake.appends / max(elapsed, 1e-9) * 60
    if wr > 60:
        verdict("WARN", "quota",
                f"one station alone issued {wr:.0f} writes/min — over "
                "Google's 60/min/user.")
    else:
        fit = max(1, int(60 // max(wr, 1)))
        verdict("PASS", "quota",
                f"one station sustained {wr:.0f} write calls/min under "
                f"saturation; ~{fit} station(s) fit inside Google's shared "
                "60/min/user budget before the adaptive gap widens.")

    dq._drop_thread_conn()


# --------------------------------------------------------------------------- #
#  PHASE 10 — soak: sustained scanning, latency drift, resource growth
# --------------------------------------------------------------------------- #
def phase10_soak(ui_ok: bool) -> None:
    rule("PHASE 10 — soak test: does latency DRIFT over a long session?")
    if not ui_ok:
        say("SKIPPED — no usable display (soak needs the real handler).")
        return
    say("Fires thousands of scans back-to-back and compares the first")
    say("decile to the last. Upward drift means a leak somewhere.\n")

    n_people = 600 if QUICK else 2_000
    total = 1_200 if QUICK else 6_000
    app, store, dq, path = _make_app(n_people)
    pids = store.roster_ids()

    lat: list[float] = []
    t_start = time.perf_counter()
    for i in range(total):
        pid = pids[i % len(pids)]
        app.entry.delete(0, "end")
        app.entry.insert(0, pid)
        app.meal_var.set(bc.MEALS[(i // len(pids)) % 12])
        t = time.perf_counter()
        app._scan_impl()
        lat.append((time.perf_counter() - t) * 1000.0)
        if i % 100 == 0:
            app.update()
    wall = time.perf_counter() - t_start

    dec = max(1, total // 10)
    first, last = lat[:dec], lat[-dec:]
    drift = statistics.median(last) - statistics.median(first)

    table([
        Stat("first 10% of scans", first),
        Stat("last  10% of scans", last),
        Stat("all scans", lat),
    ])

    db_mb = os.path.getsize(path) / 1e6 if os.path.exists(path) else 0.0
    try:
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_mb = rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024
    except Exception:                                     # noqa: BLE001
        rss_mb = float("nan")

    say(f"\n{total:,} scans in {wall:.1f} s "
        f"({total / max(wall, 1e-9):.0f} scans/s sustained)")
    say(f"median latency drift first->last decile : {drift:+.3f} ms")
    say(f"scan_queue.db grew to                   : {db_mb:.2f} MB")
    say(f"peak process RSS                        : {rss_mb:.0f} MB")
    pending_timers = _kill_app(app)
    say(f"pending Tk 'after' timers at end        : {pending_timers}")

    if pending_timers > 200:
        say("")
        say(f"NOTE: {pending_timers} un-fired after(900) flash timers were")
        say("still queued. Each scan schedules one; the soak fires scans far")
        say("faster than 900 ms so they pile up. At a REAL scan rate (~1/s)")
        say("at most one or two are ever live, so this is a harness artefact,")
        say("not a production leak — but it does mean a burst of hundreds of")
        say("scans inside one second leaves that many timers pending.")

    if drift > 5.0:
        verdict("WARN", "soak",
                f"latency drifted +{drift:.1f} ms over {total:,} scans — "
                "something is accumulating.")
    else:
        verdict("PASS", "soak",
                f"latency drift {drift:+.1f} ms over {total:,} scans — "
                "stable.")

    _kill_app(app)
    dq._drop_thread_conn()
    gc.collect()


# --------------------------------------------------------------------------- #
#  PHASE 11 — degraded modes: does anything get slow when things break?
# --------------------------------------------------------------------------- #
def phase11_degraded() -> None:
    rule("PHASE 11 — degraded modes (disk failure, offline backlog)")
    say("Latency of the fallback tiers your code drops into under failure.\n")

    people = 300 if QUICK else 800
    iters = 300 if QUICK else 1_000

    # MemoryQueue path (SQLite completely unavailable).
    write_registry(people)
    mq = bc.MemoryQueue()
    store_m = bc.Store(mq)
    pids = store_m.roster_ids()
    s_mem = []
    for i in range(iters):
        rec = {"scan_id": f"M-{i:08d}",
               "ts": datetime.now().strftime(bc.TS_FMT),
               "pid": pids[i % len(pids)], "name": "X",
               "meal": bc.MEALS[i % 12], "status": "COLLECTED",
               "machine": "BENCH"}
        t = time.perf_counter_ns()
        store_m.scan_persist(rec)
        s_mem.append((time.perf_counter_ns() - t) / 1e6)

    # MemoryQueue.pending_count() is an O(N) scan — measure as it fills.
    s_mcnt = []
    for _ in range(200):
        t = time.perf_counter_ns()
        store_m.unsynced_count()
        s_mcnt.append((time.perf_counter_ns() - t) / 1e6)

    # Emergency JSONL tier (fsync per line).
    s_emerg = []
    for i in range(min(iters, 400)):
        rec = {"scan_id": f"E-{i:08d}",
               "ts": datetime.now().strftime(bc.TS_FMT),
               "pid": "ISMUN-0001", "name": "X", "meal": "Lunch 1",
               "status": "COLLECTED", "machine": "BENCH"}
        t = time.perf_counter_ns()
        bc.emergency_append(rec)
        s_emerg.append((time.perf_counter_ns() - t) / 1e6)

    # Offline backlog: reconcile's pending(ALL) with a big unsynced queue.
    store_b, dq_b, _ = fresh_store(people)
    con = sqlite3.connect(dq_b.path)
    con.execute("PRAGMA synchronous=OFF")
    backlog = 2_000 if QUICK else 10_000
    con.executemany(
        "INSERT INTO scans(scan_id, ts, pid, name, meal, status, machine,"
        " synced) VALUES (?,?,?,?,?,?,?,0)",
        [(f"B-{i:09d}", datetime(2026, 8, 1).strftime(bc.TS_FMT),
          f"ISMUN-{(i % people) + 1:04d}", "N", bc.MEALS[i % 12],
          "COLLECTED", "BENCH") for i in range(backlog)])
    con.commit()
    con.close()
    s_backlog = []
    for _ in range(3 if QUICK else 6):
        t = time.perf_counter_ns()
        store_b.reconcile([])
        s_backlog.append((time.perf_counter_ns() - t) / 1e6)

    table([
        Stat("scan_persist() on MemoryQueue (no disk)", s_mem),
        Stat(f"unsynced_count() w/ {iters:,} in MemoryQueue", s_mcnt),
        Stat("emergency_append() JSONL + fsync", s_emerg),
        Stat(f"reconcile() w/ {backlog:,}-row OFFLINE backlog", s_backlog),
    ])

    say(f"\nMemoryQueue.pending_count() is a Python scan over every row, so")
    say(f"it grows linearly while the disk is dead. At {iters:,} rows it is")
    say(f"{pct(s_mcnt, .5):.3f} ms per scan.")
    if pct(s_backlog, .99) > 500:
        verdict("WARN", "degraded",
                f"a {backlog:,}-row offline backlog makes reconcile take "
                f"{pct(s_backlog, .99):.0f} ms every poll.")
    else:
        verdict("PASS", "degraded",
                f"offline backlog of {backlog:,} rows: reconcile still "
                f"{pct(s_backlog, .99):.0f} ms.")

    dq_b._drop_thread_conn()


# --------------------------------------------------------------------------- #
#  Cleanup + integrity verification
# --------------------------------------------------------------------------- #
def cleanup_and_verify() -> None:
    rule("CLEANUP & INTEGRITY VERIFICATION")

    created = []
    for root, dirs, files in os.walk(SANDBOX):
        for f in files:
            created.append(os.path.join(root, f))
    total_mb = sum(os.path.getsize(p) for p in created if os.path.exists(p))
    total_mb /= 1e6
    say(f"Sandbox           : {SANDBOX}")
    say(f"Files created     : {len(created)}  ({total_mb:.1f} MB)")

    if ARGS.keep:
        say("KEPT on request (--keep). Delete it yourself when done.")
    else:
        shutil.rmtree(SANDBOX, ignore_errors=True)
        gone = not os.path.exists(SANDBOX)
        say(f"Sandbox removed   : {'YES' if gone else 'NO — remove manually!'}")
        if not gone:
            verdict("FAIL", "cleanup", f"could not remove {SANDBOX}")

    fp_after = fingerprint(PROTECTED)
    changed = [p for p in PROTECTED if FP_BEFORE.get(p) != fp_after.get(p)]
    say("\nProduction files checked for tampering:")
    for p in PROTECTED:
        state = FP_BEFORE.get(p)
        mark = "CHANGED!" if p in changed else (
            "unchanged" if state else "absent (nothing to protect)")
        say(f"  [{mark:^32}] {p}")
    if changed:
        verdict("FAIL", "safety",
                f"{len(changed)} production file(s) were modified: {changed}")
    else:
        verdict("PASS", "safety",
                "no production file was created, modified, or deleted.")

    say(f"\nReal SheetClient constructed: "
        f"{'NO — zero network calls made.' if bc.SheetClient is _ForbiddenSheetClient else 'YES — INVESTIGATE'}")


def summary() -> None:
    rule("SUMMARY")
    order = {"FAIL": 0, "WARN": 1, "PASS": 2}
    for lvl, area, msg in sorted(VERDICTS, key=lambda v: order.get(v[0], 3)):
        say(f"  [{lvl:^4}] {area:<12} {msg}")
    fails = sum(1 for v in VERDICTS if v[0] == "FAIL")
    warns = sum(1 for v in VERDICTS if v[0] == "WARN")
    say("")
    if fails:
        say(f"RESULT: {fails} FAIL, {warns} WARN — investigate above.")
    elif warns:
        say(f"RESULT: {warns} WARN, no failures — usable, see notes.")
    else:
        say("RESULT: all checks passed.")


# --------------------------------------------------------------------------- #
#  Main
# --------------------------------------------------------------------------- #
def main() -> int:
    t0 = time.perf_counter()
    say("=" * 78)
    say("  ISMUN barcode.py — LATENCY & SCALING BENCHMARK")
    say("=" * 78)
    say(f"barcode.py under test : {BARCODE_PATH}")
    say(f"meals configured      : {len(bc.MEALS)}  -> {bc.MEALS}")
    say(f"board columns         : {len(bc.COLUMNS)}")
    say(f"sandbox               : {SANDBOX}")
    say(f"mode                  : {'QUICK' if QUICK else 'FULL'}")
    say(f"python                : {sys.version.split()[0]} on {sys.platform}")
    say(f"started               : {datetime.now():%Y-%m-%d %H:%M:%S}")

    ui_ok = False
    if not ARGS.no_ui:
        try:
            _probe = bc.tk.Tk()
            _probe.withdraw()
            _probe.destroy()
            ui_ok = True
        except Exception as e:                            # noqa: BLE001
            say(f"\n!! Tk unavailable ({e!r}) — UI phases will be skipped.")
            say("   On a headless Linux box try:  xvfb-run -a python3 "
                "bench_barcode.py")
    else:
        say("\nUI phases disabled by --no-ui.")

    try:
        phase1_micro()
        phase2_queue_scaling()
        phase3_reconcile()
        phase4_contention()
        phase5_board(ui_ok)
        phase6_perceived(ui_ok)
        phase7_big_state(ui_ok)
        phase8_ui_pump(ui_ok)
        phase9_pipeline()
        phase10_soak(ui_ok)
        phase11_degraded()
    except KeyboardInterrupt:
        say("\n!! interrupted by user — cleaning up anyway")
    except Exception:                                     # noqa: BLE001
        import traceback
        say("\n!! benchmark raised — cleaning up anyway\n")
        say(traceback.format_exc())
        verdict("FAIL", "harness", "the benchmark itself crashed (see above)")
    finally:
        cleanup_and_verify()
        summary()
        say(f"\nTotal wall time: {time.perf_counter() - t0:.1f} s")
        if ARGS.report:
            try:
                with open(ARGS.report, "w", encoding="utf-8") as f:
                    f.write("\n".join(_OUT) + "\n")
                print(f"\nReport written to {ARGS.report}")
            except OSError as e:
                print(f"\ncould not write report: {e!r}")

    return 1 if any(v[0] == "FAIL" for v in VERDICTS) else 0


if __name__ == "__main__":
    sys.exit(main())
