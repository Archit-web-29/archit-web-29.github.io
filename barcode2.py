#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ISMUN barcode meal logger — local-first scans, Google-Sheet-as-truth board.

v4 changes (on top of the v3 latency/resilience rewrite)
--------------------------------------------------------
1. SHEET IS THE SOURCE OF TRUTH (requirements 2, 3, 4).
   The poller now does a FULL read of the Scan Log every POLL_INTERVAL_S
   (5 s) and REBUILDS the board state from it (reconcile), instead of
   incrementally merging only new rows. Consequences:
     - Rows DELETED in the sheet disappear from every machine's UI
       within one poll (an admin can now "un-serve" someone).
     - Rows ADDED or EDITED manually in the sheet (even without a ScanID)
       appear within one poll.
     - If the whole tab is wiped, the board clears AND the header/formulas
       are self-healed.
   Local scans that have not yet PROVABLY landed in the sheet are overlaid
   on top of the sheet truth so a fresh scan never flickers off the board:
     overlay = unsynced queue ∪ mem-pending ∪ recently-synced (≤30 s)
             ∪ recent local decides (≤30 s).
   Once a scan is durably in the sheet and past the overlay horizon, the
   sheet alone decides whether it exists.

2. VIRTUALIZED CANVAS BOARD (requirement 1).
   The old board was a grid of tk.Label widgets — 13 per person. At a few
   hundred people that is thousands of live widgets: slow to build, slow to
   scroll, and creating 13 widgets inside the scan handler was the source
   of the 10-20 ms latency outliers (requirement 5). The new VirtualBoard
   draws on a single Canvas and only ever materialises the ~40 visible
   rows, recycling them on scroll. Costs are now independent of roster
   size: a 10,000-person roster scrolls and paints the same as 10.
   The scan handler paints ONLY the scanned row before flushing pixels
   (the perceived-latency point), then completes the viewport.

3. ADAPTIVE WRITE PACING.
   Your load test hit Google's 60-writes/min/user quota with three
   machines flushing every 2 s. The flusher now adapts: every 429 widens
   the per-machine flush gap (up to 15 s), every success decays it back
   toward 2 s. N machines converge on a fair share of the quota instead
   of retry-storming. (Scans are unaffected — they are always local.)

Perceived-latency budget is unchanged: decide in memory -> paint the one
row -> flush pixels (<1 ms), then fsync + everything else.

ENV VARS
--------
SCANNER_ID        station name (default: hostname)
SCANNER_DB        SQLite path (default: scan_queue.db)
SCANNER_SA        service-account JSON path (overrides the default below)
SCANNER_LOG       log file path (default: scanner.log)
SCANNER_EMERGENCY emergency JSONL path (default: scan_emergency.jsonl)
SCANNER_PERF=1    log per-scan perceived latency + rolling average



Main framework created by mimo!
Code debugged and optimised by Fable 5 extra
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import queue
import random
import re
import socket
import sqlite3
import threading
import time
import uuid
import tkinter as tk
from collections import deque
from datetime import datetime
from pathlib import Path
from time import perf_counter
from tkinter import ttk, messagebox
from typing import Any, Callable

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --------------------------------------------------------------------------- #
#  Configuration
# --------------------------------------------------------------------------- #
def _find_service_account() -> str:
    """Locate the ismun-barcode-* service-account JSON.
    Search order: SCANNER_SA env var, then home dir, then Downloads,
    then ISMUN_BARCODES under Downloads. Newest match wins. Works on
    macOS (/Users/<name>) and Windows (C:\\Users\\<name>) unchanged."""
    env = os.environ.get("SCANNER_SA")
    if env:
        return env
    home = Path.home()
    candidates: list[Path] = []
    for folder in (home, home / "Downloads",
                   home / "Downloads" / "ISMUN_BARCODES"):
        try:
            candidates.extend(folder.glob("ismun-barcode-*.json"))
        except OSError:
            continue
    if not candidates:
        # deliberate non-existent path -> SheetClient raises its clear
        # "not found. Set SCANNER_SA or fix the path" error
        return str(home / "Downloads" / "ismun-barcode-MISSING.json")
    return str(max(candidates, key=lambda p: p.stat().st_mtime))


SERVICE_ACCOUNT_FILE = _find_service_account()
SPREADSHEET_ID = "10L7U9NM38Ge17KpJOTD-DaWjGg4QQX03gz_esKZfQE0"
REGISTRY_FILE  = str(Path.home() / "Downloads" / "ISMUN_BARCODES"
                     / "id_registry.json")

MACHINE_ID     = os.environ.get("SCANNER_ID") or socket.gethostname()
DB_FILE        = os.environ.get("SCANNER_DB") or "scan_queue.db"
LOG_FILE       = os.environ.get("SCANNER_LOG") or "scanner.log"
EMERGENCY_FILE = os.environ.get("SCANNER_EMERGENCY") or "scan_emergency.jsonl"
PERF_LOG       = os.environ.get("SCANNER_PERF") == "1"

# --- pipeline tuning ------------------------------------------------------- #
FLUSH_WINDOW_S   = 1.0    # let a burst of scans accumulate before one append
MIN_FLUSH_GAP_S  = 2.0    # baseline write pacing (adaptive: grows on 429s)
MAX_FLUSH_GAP_S  = 15.0   # ceiling for the adaptive gap
MAX_BATCH        = 100    # rows per values.append call
POLL_INTERVAL_S  = 5.0    # full sheet re-read + reconcile cadence
BACKOFF_BASE_S   = 1.0    # exponential backoff seed on failures
BACKOFF_CAP_S    = 60.0
OVERLAY_HORIZON_S = 30.0  # how long a local scan outranks a stale sheet read

MEALS = ["Breakfast 1", "Lunch 1", "Desert 1", "High tea 1",
         "Breakfast 2", "Lunch 2", "Desert 2", "High tea 2",
         "Breakfast 3", "Lunch 3", "Desert 3", "High tea 3"]
MEAL_SET = frozenset(MEALS)
COLUMNS = ["ID", "Name", "Delegation Committee", "OC", "Designation"] + MEALS  # A..Q (17)

BOARD_SHEET = "Board"
LOG_SHEET   = "Scan Log"
LOG_HEADER  = ["Timestamp", "ID", "Name", "Meal", "Status", "ScanID", "Machine"]
TS_FMT      = "%Y-%m-%d %H:%M:%S"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

BG     = "#0f172a"
CARD   = "#1e293b"
INPUT  = "#172033"
ACCENT = "#38bdf8"
GOOD   = "#22c55e"
WARN   = "#f59e0b"
BAD    = "#ef4444"
TEXT   = "#e2e8f0"
MUTED  = "#94a3b8"
GRID   = "#0b1220"
CELL_GREEN = "#15803d"
FLASH      = "#22c55e"
FLASH_FG   = "#04121f"
GREEN_FG   = "#ecfdf5"

# board geometry (pixels)
ROW_H    = 26
HEADER_H = 30
COL_PX   = [88, 168, 190, 110, 110] + [92] * len(MEALS)

OC_TEAMS = {"CAMP", "DESIGN", "LOGISTICS", "MARKETING",
            "PHOTOGRAPHY", "PRESS", "TECH"}

_NUM_SUFFIX = re.compile(r"(\d+)$")

# --------------------------------------------------------------------------- #
#  Logging
# --------------------------------------------------------------------------- #
logger = logging.getLogger("scanner")


def setup_logging() -> None:
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s [%(threadName)s] %(message)s")
    try:
        fh = logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3,
            encoding="utf-8")
        fh.setFormatter(fmt)
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)
    except OSError as e:
        print(f"[log] cannot open {LOG_FILE!r}: {e!r} — console logging only")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)


def classify_error(e: BaseException) -> str:
    """Bucket an exception for backoff/telemetry decisions."""
    if isinstance(e, HttpError):
        status = getattr(getattr(e, "resp", None), "status", None)
        if status == 429:
            return "rate-limit"
        if status in (401, 403):
            return "auth"
        if status is not None and status >= 500:
            return "server"
        return f"http-{status}"
    if isinstance(e, (socket.timeout, TimeoutError, ConnectionError, OSError)):
        return "network"
    return "unexpected"


# --------------------------------------------------------------------------- #
#  Registry
# --------------------------------------------------------------------------- #
def load_registry() -> dict[str, dict[str, Any]]:
    if os.path.exists(REGISTRY_FILE):
        try:
            with open(REGISTRY_FILE, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                logger.error("registry root is %s, expected dict — ignoring",
                             type(data).__name__)
                return {}
            return data
        except (OSError, json.JSONDecodeError) as e:
            logger.error("registry load failed: %r — using empty registry", e)
            return {}
    return {}


def _registry_mtime() -> float:
    try:
        return os.path.getmtime(REGISTRY_FILE)
    except OSError:
        return 0.0


def build_numeric_index(registry: dict[str, Any]) -> dict[int, str | None]:
    """Numeric-suffix -> id. Ambiguous suffixes map to None (rejected)."""
    idx: dict[int, str | None] = {}
    for rid in registry:
        m = _NUM_SUFFIX.search(rid)
        if not m:
            continue
        n = int(m.group(1))
        idx[n] = None if n in idx else rid
    return idx


# --------------------------------------------------------------------------- #
#  Emergency JSONL — tier-2 persistence (only touched when SQLite fails)
# --------------------------------------------------------------------------- #
def emergency_append(rec: dict[str, Any]) -> bool:
    try:
        with open(EMERGENCY_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        return True
    except OSError:
        logger.critical("EMERGENCY LOG WRITE FAILED — scan is in memory only",
                        exc_info=True)
        return False


def import_emergency(dq: "DurableQueue | MemoryQueue") -> int:
    if not os.path.exists(EMERGENCY_FILE):
        return 0
    imported = 0
    try:
        with open(EMERGENCY_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if dq.insert_ignore(rec):
                        imported += 1
                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    logger.error("emergency row skipped (%r): %.120s", e, line)
        os.replace(EMERGENCY_FILE,
                   f"{EMERGENCY_FILE}.imported-{int(time.time())}")
    except OSError:
        logger.exception("emergency import failed — file left in place")
    return imported


# --------------------------------------------------------------------------- #
#  Durable local queue — SQLite, tier-1 persistence
# --------------------------------------------------------------------------- #
class DurableQueue:
    durable = True

    def __init__(self, path: str = DB_FILE):
        self.path = path
        self._local = threading.local()
        try:
            self._init_schema()
        except sqlite3.DatabaseError:
            logger.critical("scan DB corrupt/unusable — quarantining and "
                            "recreating", exc_info=True)
            self._quarantine()
            self._init_schema()

    def _quarantine(self) -> None:
        self._drop_thread_conn()
        for suffix in ("", "-wal", "-shm"):
            src = self.path + suffix
            if os.path.exists(src):
                dst = f"{src}.corrupt-{int(time.time())}"
                try:
                    os.replace(src, dst)
                    logger.critical("quarantined %s -> %s", src, dst)
                except OSError:
                    logger.critical("could not quarantine %s", src,
                                    exc_info=True)

    def _drop_thread_conn(self) -> None:
        con = getattr(self._local, "con", None)
        if con is not None:
            try:
                con.close()
            except sqlite3.Error:
                pass
            self._local.con = None

    def _init_schema(self) -> None:
        con = self._conn()
        ok = con.execute("PRAGMA quick_check").fetchone()[0]
        if ok != "ok":
            raise sqlite3.DatabaseError(f"quick_check: {ok}")
        con.execute("""
            CREATE TABLE IF NOT EXISTS scans (
                seq     INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id TEXT UNIQUE NOT NULL,
                ts      TEXT NOT NULL,
                pid     TEXT NOT NULL,
                name    TEXT,
                meal    TEXT NOT NULL,
                status  TEXT NOT NULL,
                machine TEXT NOT NULL,
                synced  INTEGER NOT NULL DEFAULT 0
            )""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_unsynced "
                    "ON scans(synced) WHERE synced = 0")
        con.commit()

    def _conn(self) -> sqlite3.Connection:
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(self.path)
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=FULL")   # fsync per commit —
            con.execute("PRAGMA busy_timeout=5000")  # off the perceived path
            self._local.con = con
        return con

    def enqueue(self, rec: dict[str, Any]) -> None:
        con = self._conn()
        con.execute(
            "INSERT INTO scans(scan_id, ts, pid, name, meal, status, machine)"
            " VALUES (?,?,?,?,?,?,?)",
            (rec["scan_id"], rec["ts"], rec["pid"], rec["name"],
             rec["meal"], rec["status"], rec["machine"]))
        con.commit()

    def insert_ignore(self, rec: dict[str, Any]) -> bool:
        con = self._conn()
        cur = con.execute(
            "INSERT OR IGNORE INTO "
            "scans(scan_id, ts, pid, name, meal, status, machine)"
            " VALUES (?,?,?,?,?,?,?)",
            (rec["scan_id"], rec["ts"], rec["pid"], rec.get("name", ""),
             rec["meal"], rec["status"], rec["machine"]))
        con.commit()
        return cur.rowcount > 0

    def pending(self, limit: int = MAX_BATCH) -> list[dict[str, Any]]:
        cur = self._conn().execute(
            "SELECT scan_id, ts, pid, name, meal, status, machine"
            " FROM scans WHERE synced=0 ORDER BY seq LIMIT ?", (limit,))
        cols = ("scan_id", "ts", "pid", "name", "meal", "status", "machine")
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def mark_synced(self, scan_ids: list[str]) -> None:
        if not scan_ids:
            return
        con = self._conn()
        qmarks = ",".join("?" * len(scan_ids))
        con.execute(f"UPDATE scans SET synced=1 WHERE scan_id IN ({qmarks})",
                    scan_ids)
        con.commit()

    def pending_count(self) -> int:
        cur = self._conn().execute("SELECT COUNT(*) FROM scans WHERE synced=0")
        return int(cur.fetchone()[0])

    def all_rows(self) -> list[dict[str, Any]]:
        cur = self._conn().execute(
            "SELECT scan_id, ts, pid, name, meal, status, machine, synced"
            " FROM scans ORDER BY seq")
        cols = ("scan_id", "ts", "pid", "name", "meal",
                "status", "machine", "synced")
        return [dict(zip(cols, row)) for row in cur.fetchall()]


class MemoryQueue:
    """Last-resort stand-in when SQLite cannot be initialised at all.
    Same interface, NON-durable — the UI shows a permanent red banner."""

    durable = False

    def __init__(self) -> None:
        self._rows: list[dict[str, Any]] = []
        self._ids: set[str] = set()
        self._lock = threading.Lock()

    def enqueue(self, rec: dict[str, Any]) -> None:
        with self._lock:
            self._rows.append({**rec, "synced": 0})
            self._ids.add(rec["scan_id"])

    def insert_ignore(self, rec: dict[str, Any]) -> bool:
        with self._lock:
            if rec["scan_id"] in self._ids:
                return False
        self.enqueue(rec)
        return True

    def pending(self, limit: int = MAX_BATCH) -> list[dict[str, Any]]:
        with self._lock:
            out = [dict(r) for r in self._rows if not r["synced"]][:limit]
        for r in out:
            r.pop("synced", None)
        return out

    def mark_synced(self, scan_ids: list[str]) -> None:
        wanted = set(scan_ids)
        with self._lock:
            for r in self._rows:
                if r["scan_id"] in wanted:
                    r["synced"] = 1

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for r in self._rows if not r["synced"])

    def all_rows(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._rows]


# --------------------------------------------------------------------------- #
#  Board formula — unchanged (the sheet's own Board tab still self-maintains)
# --------------------------------------------------------------------------- #

# Board meal columns start at F (0-based index 5) and run len(MEALS) wide.
FIRST_MEAL_COL_IDX = 5                                   # A=0 … F=5
MEAL_COL_LETTERS = [chr(ord("A") + FIRST_MEAL_COL_IDX + i)
                    for i in range(len(MEALS))]          # F..Q for 12 meals
LAST_MEAL_COL = MEAL_COL_LETTERS[-1]
    
def _meal_formula(col_letter: str) -> str:
    L = "'" + LOG_SHEET + "'"
    return (
        '=ARRAYFORMULA(IF($A2:$A="","",'
        'IFERROR(REGEXEXTRACT(VLOOKUP($A2:$A&"§"&' + col_letter + '$1,'
        'FILTER({' + L + '!$B$2:$B&"§"&' + L + '!$D$2:$D,'
        + L + '!$A$2:$A},'
        + L + '!$E$2:$E="COLLECTED"),2,FALSE),'
        '"\\d\\d:\\d\\d:\\d\\d"),"")))'
    )


# --------------------------------------------------------------------------- #
#  Google Sheets client
# --------------------------------------------------------------------------- #
class SheetClient:
    def __init__(self) -> None:
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            raise RuntimeError(
                f"'{SERVICE_ACCOUNT_FILE}' not found. Set SCANNER_SA or fix "
                "the path at the top of this file.")
        if not SPREADSHEET_ID.strip():
            raise RuntimeError(
                "SPREADSHEET_ID is blank. Create a sheet, share it with the "
                "service account's client_email as Editor, and paste its ID.")
        self._creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        self._local = threading.local()
        self.sid = SPREADSHEET_ID.strip()
        self._setup()

    @property
    def svc(self):  # type: ignore[no-untyped-def]
        s = getattr(self._local, "svc", None)
        if s is None:
            s = build("sheets", "v4", credentials=self._creds,
                      cache_discovery=False)
            self._local.svc = s
        return s

    def _setup(self) -> None:
        try:
            meta = self.svc.spreadsheets().get(spreadsheetId=self.sid).execute()
        except HttpError as e:
            raise RuntimeError(
                f"Could not open sheet '{self.sid}'. Check the ID and that "
                "the sheet is shared (Editor) with the service account's "
                f"client_email. Original error: {e}") from e

        def titles_of(m: dict[str, Any]) -> dict[str, int]:
            return {s["properties"]["title"]: s["properties"]["sheetId"]
                    for s in m.get("sheets", [])}

        titles = titles_of(meta)
        reqs = [{"addSheet": {"properties": {"title": t}}}
                for t in (LOG_SHEET, BOARD_SHEET) if t not in titles]
        if reqs:
            self.svc.spreadsheets().batchUpdate(
                spreadsheetId=self.sid, body={"requests": reqs}).execute()
            meta = self.svc.spreadsheets().get(
                spreadsheetId=self.sid).execute()
            titles = titles_of(meta)
        if "Sheet1" in titles and len(titles) > 1:
            self.svc.spreadsheets().batchUpdate(
                spreadsheetId=self.sid,
                body={"requests": [
                    {"deleteSheet": {"sheetId": titles["Sheet1"]}}]}).execute()
            del titles["Sheet1"]

        self.svc.spreadsheets().values().update(
            spreadsheetId=self.sid, range=f"{LOG_SHEET}!A1",
            valueInputOption="RAW", body={"values": [LOG_HEADER]}).execute()
        self.svc.spreadsheets().values().update(
            spreadsheetId=self.sid, range=f"{BOARD_SHEET}!A1",
            valueInputOption="RAW", body={"values": [COLUMNS]}).execute()

        self._ensure_board_machinery(titles[BOARD_SHEET])

    def re_setup(self) -> None:
        """Recovery hook: tab deleted/renamed/wiped mid-event."""
        self._setup()

    def verify_and_heal(self) -> bool:
        """Cheap header probe; full re-setup if the Scan Log was wiped.
        Returns True if healing ran."""
        res = self.svc.spreadsheets().values().get(
            spreadsheetId=self.sid, range=f"{LOG_SHEET}!A1:A1").execute()
        cell = (res.get("values") or [[""]])[0][0] if res.get("values") else ""
        if cell != LOG_HEADER[0]:
            logger.warning("Scan Log header missing — re-initialising "
                           "workbook (was the sheet erased?)")
            self._setup()
            return True
        return False

    def _ensure_board_machinery(self, board_sheet_id: int) -> None:
        # Probe the WHOLE meal-formula strip, not just F2. A sheet built by
        # the old 9-meal build has F2 populated but O2:Q2 empty — probing F2
        # alone would early-return and leave the three Desert columns dead.
        res = self.svc.spreadsheets().values().get(
            spreadsheetId=self.sid,
            range=f"{BOARD_SHEET}!F2:{LAST_MEAL_COL}2",
            valueRenderOption="FORMULA").execute()
        vals = res.get("values") or []
        row = list(vals[0]) if vals else []
        row += [""] * (len(MEALS) - len(row))
        if all(str(c).startswith("=ARRAYFORMULA") for c in row):
            return

        self.svc.spreadsheets().values().update(
            spreadsheetId=self.sid,
            range=f"{BOARD_SHEET}!F2:{LAST_MEAL_COL}2",
            valueInputOption="USER_ENTERED",
            body={"values": [[_meal_formula(c)
                              for c in MEAL_COL_LETTERS]]}).execute()

        header_blue = {"red": 0.122, "green": 0.306, "blue": 0.470}
        green_fill  = {"red": 0.776, "green": 0.937, "blue": 0.808}
        requests = [
            {
                "repeatCell": {
                    "range": {"sheetId": board_sheet_id,
                              "startRowIndex": 0, "endRowIndex": 1,
                              "startColumnIndex": 0,
                              "endColumnIndex": len(COLUMNS)},
                    "cell": {"userEnteredFormat": {
                        "backgroundColor": header_blue,
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"bold": True, "foregroundColor":
                                       {"red": 1, "green": 1, "blue": 1}}}},
                    "fields": "userEnteredFormat(backgroundColor,"
                              "horizontalAlignment,verticalAlignment,"
                              "textFormat)",
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": board_sheet_id,
                                   "gridProperties": {"frozenRowCount": 1,
                                                      "frozenColumnCount": 2}},
                    "fields": "gridProperties(frozenRowCount,"
                              "frozenColumnCount)",
                }
            },
            {
                "addConditionalFormatRule": {
                    "index": 0,
                    "rule": {
                        "ranges": [{"sheetId": board_sheet_id,
                                    "startRowIndex": 1, "endRowIndex": 10000,
                                    "startColumnIndex": FIRST_MEAL_COL_IDX,
                                    "endColumnIndex": len(COLUMNS)}],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": '=F2<>""'}]},
                            "format": {"backgroundColor": green_fill}},
                    },
                }
            },
        ]
        self.svc.spreadsheets().batchUpdate(
            spreadsheetId=self.sid, body={"requests": requests}).execute()

    # ---- pipeline API ------------------------------------------------------ #
    def append_log_rows(self, values: list[list[str]]) -> None:
        self.svc.spreadsheets().values().append(
            spreadsheetId=self.sid, range=f"{LOG_SHEET}!A:G",
            valueInputOption="RAW", insertDataOption="INSERT_ROWS",
            body={"values": values}).execute()

    def read_log_all(self) -> list[list[str]]:
        """Full Scan Log read — the sheet is the source of truth."""
        res = self.svc.spreadsheets().values().get(
            spreadsheetId=self.sid,
            range=f"{LOG_SHEET}!A2:G").execute()
        return res.get("values", [])

    def push_roster(self, rows: list[list[str]]) -> None:
        self.svc.spreadsheets().values().clear(
            spreadsheetId=self.sid, range=f"{BOARD_SHEET}!A2:E",
            body={}).execute()
        if rows:
            self.svc.spreadsheets().values().update(
                spreadsheetId=self.sid, range=f"{BOARD_SHEET}!A2",
                valueInputOption="RAW", body={"values": rows}).execute()


# --------------------------------------------------------------------------- #
#  Client manager — builds SheetClient in the background with retry
# --------------------------------------------------------------------------- #
class SheetClientManager(threading.Thread):
    def __init__(self, ui_events: "queue.Queue[tuple[str, Any]]",
                 factory: Callable[[], Any] = None) -> None:  # type: ignore
        super().__init__(daemon=True, name="client-init")
        self.ui = ui_events
        self._factory = factory or SheetClient
        self._client: Any = None
        self._lock = threading.Lock()

    def get(self) -> Any:
        with self._lock:
            return self._client

    def run(self) -> None:
        backoff = BACKOFF_BASE_S
        while True:
            try:
                c = self._factory()
            except Exception as e:
                logger.error("Sheets client init failed [%s]: %r — retrying "
                             "in %.0fs (scans keep queueing locally)",
                             classify_error(e), e, backoff)
                self.ui.put(("offline-init", None))
                time.sleep(backoff + random.uniform(0, backoff))
                backoff = min(BACKOFF_CAP_S, backoff * 2)
                continue
            with self._lock:
                self._client = c
            logger.info("Sheets client ready")
            self.ui.put(("online", None))
            return


# --------------------------------------------------------------------------- #
#  Store — local state + persistence fallback chain + sheet reconciliation
# --------------------------------------------------------------------------- #
class Store:
    def __init__(self, dq: DurableQueue | MemoryQueue,
                 machine_id: str = MACHINE_ID) -> None:
        self.dq = dq
        self.machine_id = machine_id
        self.lock = threading.RLock()
        self.registry = load_registry()
        self._num_index = build_numeric_index(self.registry)
        self._reg_mtime = _registry_mtime()
        self.state: dict[str, dict[str, datetime]] = {}
        self.mem_pending: deque[dict[str, Any]] = deque()   # tier-3 records
        # Overlay sources for reconcile: local activity that may not be
        # visible in a (possibly seconds-stale) sheet snapshot yet.
        self.recent_synced: deque[tuple[float, dict[str, Any]]] = deque()
        self.recent_decides: deque[tuple[float, dict[str, Any]]] = deque()
        self.flush_event = threading.Event()

    # ---- registry ---------------------------------------------------------- #
    def reload_registry_if_changed(self) -> bool:
        m = _registry_mtime()
        if m != self._reg_mtime:
            reg = load_registry()             # parse OUTSIDE the lock
            idx = build_numeric_index(reg)
            with self.lock:
                self.registry = reg
                self._num_index = idx
                self._reg_mtime = m
            logger.info("registry reloaded (%d ids)", len(reg))
            return True
        return False

    def resolve(self, code: str) -> str | None:
        """O(1): exact id hit, else unique-numeric-suffix hit."""
        code = code.strip()
        with self.lock:
            if code in self.registry:
                return code
            if code.isdigit():
                return self._num_index.get(int(code))
        return None

    def roster_ids(self) -> list[str]:
        def key(rid: str) -> int:
            m = _NUM_SUFFIX.search(rid)
            return int(m.group(1)) if m else 0
        with self.lock:
            return sorted(self.registry, key=key)

    def person_fields(self, pid: str) -> tuple[str, str, str, str]:
        rec = self.registry.get(pid, {})
        name = rec.get("name", "")
        aff = (rec.get("committee", "") or "").strip()
        group = (rec.get("group", "") or "").strip()
        if aff.upper() == "SEC":
            deleg, oc, desig = "SEC", "SEC", "SEC"
        elif aff.upper() in OC_TEAMS:
            deleg, oc, desig = "-", aff, (group or "-")
        else:
            deleg, oc, desig = (aff or "-"), "-", (group or "-")
        return name, deleg, oc, desig

    def board_roster_rows(self) -> list[list[str]]:
        return [[pid, *self.person_fields(pid)] for pid in self.roster_ids()]

    def collection_time(self, pid: str, meal: str) -> datetime | None:
        with self.lock:
            return self.state.get(pid, {}).get(meal)

    def known_pids(self) -> list[str]:
        """Roster order, then any extra pids present only in state."""
        roster = self.roster_ids()
        with self.lock:
            extras = sorted(set(self.state) - set(roster))
        return roster + extras

    def unsynced_count(self) -> int:
        try:
            n = self.dq.pending_count()
        except Exception:
            logger.exception("pending_count failed")
            n = 0
        with self.lock:
            return n + len(self.mem_pending)

    # ---- crash recovery: provisional local truth until the first poll ------- #
    def bootstrap_local(self) -> None:
        try:
            rows = self.dq.all_rows()
        except Exception:
            logger.exception("bootstrap: could not read local queue")
            rows = []
        with self.lock:
            for r in rows:
                if r["status"] != "COLLECTED":
                    continue
                try:
                    ts = datetime.strptime(r["ts"], TS_FMT)
                except (ValueError, TypeError):
                    continue
                slot = self.state.setdefault(r["pid"], {})
                if r["meal"] not in slot or ts < slot[r["meal"]]:
                    slot[r["meal"]] = ts

    # ---- PHASE 1: the perceived scan path — pure memory --------------------- #
    def scan_decide(self, pid: str, meal: str) -> dict[str, Any]:
        now = datetime.now()
        scan_id = f"{self.machine_id}-{uuid.uuid4().hex[:12]}"
        rec = {"scan_id": scan_id, "ts": now.strftime(TS_FMT),
               "pid": pid, "name": "", "meal": meal,
               "status": "", "machine": self.machine_id}
        with self.lock:
            name, _, _, _ = self.person_fields(pid)
            slot = self.state.setdefault(pid, {})
            if meal in slot:
                status, first = "ALREADY", slot[meal]
            else:
                slot[meal] = now
                status, first = "COLLECTED", now
            rec["name"], rec["status"] = name, status
            if status == "COLLECTED":
                # Overlay source: protects this scan from being wiped by a
                # reconcile built from a sheet snapshot that predates it.
                self.recent_decides.append((time.monotonic(), rec))
        return {"status": status, "first": first, "name": name, "rec": rec}

    # ---- PHASE 2: durability, three-tier fallback chain ---------------------- #
    def scan_persist(self, rec: dict[str, Any]) -> str:
        try:
            self.dq.enqueue(rec)
            return "sqlite"
        except Exception:
            logger.exception("SQLite enqueue failed — falling back "
                             "(scan_id=%s)", rec["scan_id"])
        with self.lock:
            self.mem_pending.append(rec)
        return "emergency" if emergency_append(rec) else "memory"

    def drop_mem_pending(self, scan_ids: set[str]) -> None:
        with self.lock:
            self.mem_pending = deque(
                r for r in self.mem_pending if r["scan_id"] not in scan_ids)

    def note_synced(self, recs: list[dict[str, Any]]) -> None:
        now = time.monotonic()
        with self.lock:
            self.recent_synced.extend((now, r) for r in recs)

    # ---- SHEET-AS-TRUTH RECONCILIATION -------------------------------------- #
    def reconcile(self, sheet_rows: list[list[str]]) -> set[tuple[str, str]]:
        """Rebuild board state from the sheet (truth), overlaid with local
        scans that may not be visible in this snapshot yet. Returns every
        (pid, meal) cell whose value changed — including cells that were
        CLEARED because their rows were deleted from the sheet."""
        new_state: dict[str, dict[str, datetime]] = {}
        seen_ids: set[str] = set()

        def merge(pid: str, meal: str, ts: datetime) -> None:
            slot = new_state.setdefault(pid, {})
            if meal not in slot or ts < slot[meal]:
                slot[meal] = ts                       # earliest-wins

        # 1) The sheet — authoritative. Manual rows (no ScanID) count too.
        for raw in sheet_rows:
            try:
                r = (list(raw) + [""] * 7)[:7]
                ts_s, pid, _name, meal, status, scan_id, _machine = (
                    str(x).strip() for x in r)
            except (TypeError, ValueError):
                logger.debug("malformed sheet row skipped: %.120r", raw)
                continue
            if status != "COLLECTED" or not pid or meal not in MEAL_SET:
                continue
            if scan_id:
                if scan_id in seen_ids:               # duplicate append —
                    continue                          # idempotent, skip
                seen_ids.add(scan_id)
            try:
                ts = datetime.strptime(ts_s, TS_FMT)
            except (ValueError, TypeError):
                logger.debug("bad timestamp in sheet row skipped: %.120r", raw)
                continue
            merge(pid, meal, ts)

        # 2) Overlay — local activity the snapshot may not include yet:
        #    unsynced queue, mem-pending, recently synced, recent decides.
        overlay: list[dict[str, Any]] = []
        try:
            overlay.extend(self.dq.pending(1_000_000_000))
        except Exception:
            logger.exception("reconcile: local queue read failed")
        horizon = time.monotonic() - OVERLAY_HORIZON_S
        with self.lock:
            while self.recent_synced and self.recent_synced[0][0] < horizon:
                self.recent_synced.popleft()
            while self.recent_decides and self.recent_decides[0][0] < horizon:
                self.recent_decides.popleft()
            overlay.extend(self.mem_pending)
            overlay.extend(r for _, r in self.recent_synced)
            overlay.extend(r for _, r in self.recent_decides)

        for r in overlay:
            if r.get("status") != "COLLECTED":
                continue
            pid, meal = r.get("pid", ""), r.get("meal", "")
            if not pid or meal not in MEAL_SET:
                continue
            try:
                ts = datetime.strptime(r["ts"], TS_FMT)
            except (KeyError, ValueError, TypeError):
                continue
            merge(pid, meal, ts)

        # 3) Swap + diff under the lock (µs–ms; a concurrent scan_decide's
        #    mutation is preserved because it is in recent_decides above,
        #    or happens after the swap against the new state).
        changed: set[tuple[str, str]] = set()
        with self.lock:
            old = self.state
            for pid in old.keys() | new_state.keys():
                om, nm = old.get(pid, {}), new_state.get(pid, {})
                for meal in om.keys() | nm.keys():
                    if om.get(meal) != nm.get(meal):
                        changed.add((pid, meal))
            self.state = new_state
        return changed


# --------------------------------------------------------------------------- #
#  Flusher — the only writer. Adaptive pacing against the shared write quota.
# --------------------------------------------------------------------------- #
class Flusher(threading.Thread):
    def __init__(self, store: Store, mgr: SheetClientManager,
                 ui_events: "queue.Queue[tuple[str, Any]]") -> None:
        super().__init__(daemon=True, name="flusher")
        self.store, self.mgr, self.ui = store, mgr, ui_events
        self._backoff = BACKOFF_BASE_S
        self._gap = MIN_FLUSH_GAP_S           # adaptive: grows on 429s
        self._last_flush = 0.0

    def run(self) -> None:
        while True:
            try:
                self._cycle()
            except Exception:
                logger.exception("flusher: unexpected error — recovering")
                time.sleep(5.0)

    def _cycle(self) -> None:
        self.store.flush_event.wait(timeout=2.0)
        self.store.flush_event.clear()

        client = self.mgr.get()
        if client is None:
            return
        if self.store.unsynced_count() == 0:
            return

        time.sleep(FLUSH_WINDOW_S)
        gap = self._gap - (time.monotonic() - self._last_flush)
        if gap > 0:
            time.sleep(gap)

        try:
            batch = self.store.dq.pending(MAX_BATCH)
        except Exception:
            logger.exception("flusher: local queue read failed")
            batch = []
        with self.store.lock:
            room = MAX_BATCH - len(batch)
            mem = list(self.store.mem_pending)[:max(0, room)]
        if not batch and not mem:
            return

        rows = batch + mem
        values = [[b["ts"], b["pid"], b["name"], b["meal"],
                   b["status"], b["scan_id"], b["machine"]] for b in rows]
        try:
            client.append_log_rows(values)
        except Exception as e:
            kind = classify_error(e)
            if kind == "rate-limit":
                # Widen this machine's share of the 60 writes/min/user
                # quota; success decays it back. N machines converge on a
                # fair cadence instead of retry-storming (your load test).
                self._gap = min(MAX_FLUSH_GAP_S, self._gap * 1.5 + 1.0)
            if kind == "auth":
                logger.critical("flusher: 401/403 — check that the service "
                                "account still has Editor access")
            logger.warning("flusher: append failed [%s]: %r — backing off "
                           "%.1fs (gap now %.1fs), queue is safe locally",
                           kind, e, self._backoff, self._gap)
            self.ui.put(("offline", self.store.unsynced_count()))
            time.sleep(self._backoff + random.uniform(0, self._backoff))
            self._backoff = min(BACKOFF_CAP_S, self._backoff * 2)
            self.store.flush_event.set()
            return

        try:
            self.store.dq.mark_synced([b["scan_id"] for b in batch])
        except Exception:
            logger.exception("flusher: mark_synced failed — duplicate "
                             "appends possible (idempotent, board unaffected)")
        self.store.drop_mem_pending({b["scan_id"] for b in mem})
        self.store.note_synced(rows)          # overlay: no flicker vs stale
                                              # sheet snapshots
        self._last_flush = time.monotonic()
        self._backoff = BACKOFF_BASE_S
        self._gap = max(MIN_FLUSH_GAP_S, self._gap * 0.9)
        n = self.store.unsynced_count()
        self.ui.put(("synced", n))
        if n:
            self.store.flush_event.set()


# --------------------------------------------------------------------------- #
#  Poller — full read + reconcile every POLL_INTERVAL_S. The sheet is truth.
# --------------------------------------------------------------------------- #
class Poller(threading.Thread):
    def __init__(self, store: Store, mgr: SheetClientManager,
                 ui_events: "queue.Queue[tuple[str, Any]]") -> None:
        super().__init__(daemon=True, name="poller")
        self.store, self.mgr, self.ui = store, mgr, ui_events
        self._delay = 0.5                     # first poll: fast
        self._roster_pushed = False
        self._had_rows = False

    def run(self) -> None:
        while True:
            time.sleep(self._delay)
            try:
                self._delay = self._cycle()
            except Exception as e:
                kind = classify_error(e)
                self._delay = min(60.0, max(self._delay, 1.0) * 2)
                logger.warning("poller: cycle failed [%s]: %r — next poll in "
                               "%.0fs", kind, e, self._delay)
                if isinstance(e, HttpError):
                    status = getattr(getattr(e, "resp", None), "status", None)
                    if status in (400, 404):
                        self._try_re_setup()

    def _try_re_setup(self) -> None:
        client = self.mgr.get()
        if client is None:
            return
        try:
            client.re_setup()
            logger.warning("poller: workbook re-initialised after 400/404")
        except Exception:
            logger.exception("poller: re-setup failed")

    def _cycle(self) -> float:
        client = self.mgr.get()
        if client is None:
            return 1.0

        if not self._roster_pushed:
            client.push_roster(self.store.board_roster_rows())
            self._roster_pushed = True

        if self.store.reload_registry_if_changed():
            client.push_roster(self.store.board_roster_rows())
            self.ui.put(("roster", None))

        rows = client.read_log_all()          # FULL read: sheet is truth
        if not rows and self._had_rows:
            # The log had data and now has none: either an admin wiped it
            # (legitimate — the board must clear) or the tab was damaged.
            # Heal the header/formulas either way; the empty truth stands.
            try:
                if client.verify_and_heal():
                    self.ui.put(("healed", None))
            except Exception:
                logger.exception("poller: heal probe failed")
        self._had_rows = bool(rows)

        changed = self.store.reconcile(rows)
        if changed:
            self.ui.put(("cells", changed))
        return POLL_INTERVAL_S


# --------------------------------------------------------------------------- #
#  VirtualBoard — a virtualized canvas table. Only the ~40 visible rows ever
#  exist as canvas items; they are recycled on scroll. All costs are O(visible)
#  and independent of roster size. Per-cell (text, bg, fg) caching means a
#  no-op render touches nothing and a single-scan render touches one row.
# --------------------------------------------------------------------------- #
class VirtualBoard(tk.Frame):
    def __init__(self, parent: tk.Misc, store: Store) -> None:
        super().__init__(parent, bg=BG)
        self.store = store
        self.order: list[str] = []
        self.pos: dict[str, int] = {}
        self.first = 0                        # index of first visible row
        self._nslots = 0
        self._slots: list[list[tuple[int, int]]] = []   # (rect_id, text_id)
        self._cache: list[list[tuple[str, str, str] | None]] = []
        self._flash: dict[str, str] = {}       # pid -> flash colour
        self._total_w = sum(COL_PX)

        self.header = tk.Canvas(self, height=HEADER_H, bg=GRID,
                                highlightthickness=0)
        self.body = tk.Canvas(self, bg=BG, highlightthickness=0)
        self.vbar = ttk.Scrollbar(self, orient="vertical",
                                  command=self._on_vbar)
        self.hbar = ttk.Scrollbar(self, orient="horizontal",
                                  command=self._xview)
        self.header.grid(row=0, column=0, sticky="ew")
        self.body.grid(row=1, column=0, sticky="nsew")
        self.vbar.grid(row=1, column=1, sticky="ns")
        self.hbar.grid(row=2, column=0, sticky="ew")
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.header.configure(scrollregion=(0, 0, self._total_w, HEADER_H))
        self.body.configure(scrollregion=(0, 0, self._total_w, 1),
                            xscrollcommand=self._on_xset)

        x = 0
        for c, title in enumerate(COLUMNS):
            w = COL_PX[c]
            self.header.create_rectangle(x, 0, x + w, HEADER_H,
                                         fill=GRID, outline=BG)
            anchor, tx = (("w", x + 8) if c in (0, 1, 2)
                          else ("center", x + w / 2))
            self.header.create_text(tx, HEADER_H / 2, text=title, fill=ACCENT,
                                    anchor=anchor,
                                    font=("Segoe UI Semibold", 10))
            x += w

        self.body.bind("<Configure>", self._on_resize)
        for w in (self.body, self.header):
            w.bind("<MouseWheel>", self._on_wheel)
            w.bind("<Button-4>", lambda e: self._wheel_steps(-3))   # X11
            w.bind("<Button-5>", lambda e: self._wheel_steps(3))

    # ---- geometry / pooling ------------------------------------------------ #
    def _visible_rows(self) -> int:
        return max(1, self.body.winfo_height() // ROW_H)

    def _on_resize(self, _e: object = None) -> None:
        need = self._visible_rows() + 1
        while self._nslots < need:
            s = self._nslots
            y0 = s * ROW_H
            row: list[tuple[int, int]] = []
            x = 0
            for c in range(len(COLUMNS)):
                w = COL_PX[c]
                r = self.body.create_rectangle(x, y0, x + w, y0 + ROW_H,
                                               fill=BG, outline=GRID)
                anchor, tx = (("w", x + 8) if c in (0, 1, 2)
                              else ("center", x + w / 2))
                t = self.body.create_text(tx, y0 + ROW_H / 2, text="",
                                          fill=TEXT, anchor=anchor,
                                          font=("Segoe UI", 10))
                row.append((r, t))
                x += w
            self._slots.append(row)
            self._cache.append([None] * len(COLUMNS))
            self._nslots += 1
        self.render()

    # ---- scrolling ---------------------------------------------------------- #
    def _clamp_first(self) -> None:
        self.first = max(0, min(self.first,
                                max(0, len(self.order) - self._visible_rows())))

    def _on_vbar(self, *args: str) -> None:
        n = len(self.order)
        if not n:
            return
        if args[0] == "moveto":
            self.first = int(float(args[1]) * n)
        elif args[0] == "scroll":
            step = int(args[1])
            if args[2] == "pages":
                step *= self._visible_rows()
            self.first += step
        self._clamp_first()
        self.render()

    def _wheel_steps(self, rows: int) -> None:
        self.first += rows
        self._clamp_first()
        self.render()

    def _on_wheel(self, e: "tk.Event[Any]") -> None:
        if e.delta:
            self._wheel_steps(-3 if e.delta > 0 else 3)

    def _xview(self, *args: str) -> None:
        self.body.xview(*args)

    def _on_xset(self, lo: str, hi: str) -> None:
        self.hbar.set(lo, hi)
        self.header.xview_moveto(lo)          # keep header aligned

    def _update_vbar(self) -> None:
        n = len(self.order)
        vis = self._visible_rows()
        if n <= vis:
            self.vbar.set(0.0, 1.0)
        else:
            self.vbar.set(self.first / n, (self.first + vis) / n)

    # ---- data model ---------------------------------------------------------- #
    def set_order(self, pids: list[str]) -> None:
        self.order = list(pids)
        self.pos = {pid: i for i, pid in enumerate(self.order)}
        self._clamp_first()
        self.render()

    def ensure_pid(self, pid: str) -> None:
        if pid not in self.pos:
            self.pos[pid] = len(self.order)
            self.order.append(pid)

    # ---- rendering ------------------------------------------------------------ #
    def _row_content(self, pid: str | None) -> list[tuple[str, str, str]]:
        if pid is None:
            return [("", BG, TEXT)] * len(COLUMNS)
        flash = self._flash.get(pid)
        name, deleg, oc, desig = self.store.person_fields(pid)
        out: list[tuple[str, str, str]] = [
            (pid, CARD, TEXT), (name, CARD, TEXT),
            (deleg, CARD, TEXT), (oc, CARD, TEXT), (desig, CARD, TEXT)]
        for meal in MEALS:
            t = self.store.collection_time(pid, meal)
            if t:
                out.append((t.strftime("%H:%M"), CELL_GREEN, GREEN_FG))
            else:
                out.append(("", CARD, TEXT))
        if flash:
            out = [(txt, flash, FLASH_FG) for txt, _, _ in out]
        return out

    def _render_slot(self, s: int) -> None:
        i = self.first + s
        pid = self.order[i] if 0 <= i < len(self.order) else None
        content = self._row_content(pid)
        cache = self._cache[s]
        cfg = self.body.itemconfigure
        for c, cell in enumerate(content):
            if cache[c] != cell:
                rect, text = self._slots[s][c]
                cfg(rect, fill=cell[1])
                cfg(text, text=cell[0], fill=cell[2])
                cache[c] = cell

    def render(self) -> None:
        self._update_vbar()
        for s in range(self._nslots):
            self._render_slot(s)

    def render_pid(self, pid: str) -> None:
        """Repaint just one row if visible — the perceived-latency path."""
        i = self.pos.get(pid)
        if i is None:
            return
        s = i - self.first
        if 0 <= s < self._nslots:
            self._render_slot(s)

    # ---- scan feedback ----------------------------------------------------------- #
    def scroll_to(self, pid: str) -> bool:
        """Bring pid into view. Returns True if the viewport moved."""
        i = self.pos.get(pid)
        if i is None:
            return False
        vis = self._visible_rows()
        if self.first <= i < self.first + vis:
            return False
        self.first = max(0, min(i - vis // 2, max(0, len(self.order) - vis)))
        return True

    def flash(self, pid: str, colour: str,
              after: Callable[[int, Callable[[], None]], object]) -> None:
        self._flash[pid] = colour
        after(900, lambda: self._end_flash(pid))

    def _end_flash(self, pid: str) -> None:
        self._flash.pop(pid, None)
        self.render_pid(pid)


# --------------------------------------------------------------------------- #
#  GUI
# --------------------------------------------------------------------------- #
class App(tk.Tk):
    def __init__(self, store: Store) -> None:
        super().__init__()
        self.store = store
        self.ui_events: "queue.Queue[tuple[str, Any]]" = queue.Queue()
        self._lat: deque[float] = deque(maxlen=200)

        self.title("Barcode Logger — local-first · Google Sheets")
        self.geometry("1200x720")
        self.minsize(980, 600)
        self.configure(bg=BG)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build_styles()
        self._build_header()
        self._build_controls()
        self.boardview = VirtualBoard(self, store)
        self.boardview.pack(fill="both", expand=True, padx=20, pady=(6, 6))
        self._build_statusbar()

        self.boardview.set_order(store.known_pids())
        self.after(200, lambda: self.entry.focus_set())
        self.after(100, self._drain_ui)

        if not getattr(store.dq, "durable", True):
            self._set_sync("⛔ DISK UNAVAILABLE — scans are NOT crash-safe",
                           BAD)
        else:
            n = store.unsynced_count()
            self._set_sync(f"⇡ {n} pending from last run" if n
                           else "● waiting for Google Sheets…",
                           WARN if n else MUTED)

    def report_callback_exception(self, exc, val, tb):  # type: ignore[override]
        logger.error("Tk callback exception", exc_info=(exc, val, tb))
        try:
            self._flash("⛔ internal UI error — details in scanner.log", BAD)
        except Exception:
            pass

    def _on_close(self) -> None:
        n = self.store.unsynced_count()
        if n and not messagebox.askokcancel(
                "Unsynced scans",
                f"{n} scan(s) have not reached Google Sheets yet.\n"
                "They are saved locally and will sync automatically the next "
                "time this app starts.\n\nQuit anyway?", parent=self):
            return
        self.destroy()

    def _build_styles(self) -> None:
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("TCombobox", foreground=TEXT, fieldbackground=INPUT,
                    background=CARD, arrowcolor=ACCENT, bordercolor=CARD,
                    padding=4)
        s.map("TCombobox",
              fieldbackground=[("readonly", INPUT)],
              foreground=[("readonly", TEXT)],
              selectbackground=[("readonly", INPUT)],
              selectforeground=[("readonly", TEXT)],
              arrowcolor=[("readonly", ACCENT)])
        self.option_add("*TCombobox*Listbox.background", CARD)
        self.option_add("*TCombobox*Listbox.foreground", TEXT)
        self.option_add("*TCombobox*Listbox.selectBackground", ACCENT)
        self.option_add("*TCombobox*Listbox.selectForeground", BG)

    def _build_header(self) -> None:
        head = tk.Frame(self, bg=BG)
        head.pack(fill="x", padx=20, pady=(16, 4))
        tk.Label(head, text="Barcode Logger", bg=BG, fg=TEXT,
                 font=("Segoe UI Semibold", 20)).pack(side="left")
        tk.Label(head, text=f"station: {MACHINE_ID} · sheet is source of truth",
                 bg=BG, fg=MUTED, font=("Segoe UI", 11)).pack(side="right",
                                                              pady=8)

    def _build_controls(self) -> None:
        bar = tk.Frame(self, bg=BG)
        bar.pack(fill="x", padx=20, pady=(4, 6))
        tk.Label(bar, text="Meal being served:", bg=BG, fg=MUTED,
                 font=("Segoe UI", 11)).pack(side="left")
        self.meal_var = tk.StringVar(value=MEALS[0])
        self.meal_box = ttk.Combobox(bar, textvariable=self.meal_var,
                                     values=MEALS, state="readonly", width=14,
                                     font=("Segoe UI Semibold", 12))
        self.meal_box.pack(side="left", padx=(8, 16))
        tk.Label(bar, text="Scan or type ID / number:", bg=BG, fg=MUTED,
                 font=("Segoe UI", 11)).pack(side="left")
        self.entry = tk.Entry(bar, font=("Consolas", 15), bg=INPUT, fg=TEXT,
                              insertbackground=ACCENT, relief="flat", width=18,
                              highlightthickness=2, highlightbackground=CARD,
                              highlightcolor=ACCENT)
        self.entry.pack(side="left", padx=(8, 0), ipady=6)
        self.entry.bind("<Return>", self._on_scan)

    def _build_statusbar(self) -> None:
        bar = tk.Frame(self, bg=GRID)
        bar.pack(fill="x", side="bottom")
        self.status = tk.Label(bar, text="Ready. Pick the meal, then scan.",
                               bg=GRID, fg=MUTED, anchor="w",
                               font=("Segoe UI", 10))
        self.status.pack(side="left", fill="x", expand=True,
                         ipady=6, padx=(10, 0))
        self.sync_lbl = tk.Label(bar, text="● starting…", bg=GRID, fg=MUTED,
                                 anchor="e", font=("Segoe UI", 10))
        self.sync_lbl.pack(side="right", ipady=6, padx=(0, 12))

    def _set_sync(self, text: str, colour: str) -> None:
        self.sync_lbl.config(text=text, fg=colour)

    def _flash(self, text: str, colour: str) -> None:
        self.status.config(text=text, fg=colour)

    # ======================================================================= #
    #  THE SCAN PATH — paint ONE row, flush pixels (perceived done), then
    #  finish the viewport, fsync, badge, perf log, and the repeat modal.
    # ======================================================================= #
    def _on_scan(self, _event: object = None) -> None:
        try:
            self._scan_impl()
        except Exception:
            logger.exception("scan handler failed")
            self.bell()
            self._flash("⛔ internal error — scan NOT recorded. "
                        "Re-scan; details in scanner.log", BAD)
            self.entry.focus_set()

    def _scan_impl(self) -> None:
        t0 = perf_counter()

        # -- Phase 1: memory + one-row paint only ---------------------------- #
        code = self.entry.get().strip()
        self.entry.delete(0, "end")
        if not code:
            return

        pid = self.store.resolve(code)
        if pid is None and self.store.reload_registry_if_changed():
            pid = self.store.resolve(code)    # rare: id just added on disk
        if pid is None:
            self.bell()
            self._flash(f"⛔  '{code}' is not a recognised barcode — "
                        "rejected, NOT recorded.", BAD)
            self.entry.focus_set()
            return

        bv = self.boardview
        bv.ensure_pid(pid)
        meal = self.meal_var.get()
        res = self.store.scan_decide(pid, meal)     # µs: pure memory
        who = f"{res['name']} ({pid})"
        if res["status"] == "COLLECTED":
            self._flash(f"✓  {who} — {meal} collected.", GOOD)
            bv.flash(pid, FLASH, self.after)
        else:
            self.bell()
            first = res["first"].strftime("%H:%M") if res["first"] else "?"
            self._flash(f"⚠  {who} ALREADY took {meal} "
                        f"(first at {first}) — do not serve again.", WARN)
            bv.flash(pid, WARN, self.after)
        moved = bv.scroll_to(pid)
        bv.render_pid(pid)                    # paint just the scanned row
        self.update_idletasks()               # pixels hit the screen HERE
        t_perceived = (perf_counter() - t0) * 1000.0
        # -- end of perceived path -------------------------------------------- #

        if moved:
            bv.render()                       # finish the rest of the viewport

        tier = self.store.scan_persist(res["rec"])
        if tier == "emergency":
            self._set_sync("⚠ DB error — scan saved to emergency log", BAD)
        elif tier == "memory":
            self._set_sync("⛔ DISK FAILURE — scan in memory ONLY", BAD)
        elif not getattr(self.store.dq, "durable", True):
            self._set_sync(f"⛔ NON-DURABLE mode — "
                           f"{self.store.unsynced_count()} pending", BAD)
        else:
            self._set_sync(f"⇡ {self.store.unsynced_count()} pending", MUTED)
        self.store.flush_event.set()

        if PERF_LOG:
            self._lat.append(t_perceived)
            avg = sum(self._lat) / len(self._lat)
            logger.info("[perf] perceived %.2f ms (avg %.2f over %d)",
                        t_perceived, avg, len(self._lat))

        self._notify(res, meal, who)
        self.entry.focus_set()

    def _notify(self, res: dict[str, Any], meal: str, who: str) -> None:
        """Modal warning ONLY for repeat servings (blocks the trailing Enter)."""
        if res["status"] == "COLLECTED":
            return
        first = res["first"].strftime("%H:%M") if res["first"] else "?"
        messagebox.showwarning(
            "Already eaten — do NOT serve",
            f"{who}\n\nThis person has ALREADY eaten {meal} "
            f"(first at {first}).\nDo not serve this meal again.",
            parent=self)

    # ----- events from the background threads (10 Hz pump) ------------------ #
    def _drain_ui(self) -> None:
        try:
            while True:
                kind, payload = self.ui_events.get_nowait()
                if kind == "cells":
                    # Reconcile result: adds AND removals. Cache-diff render
                    # repaints only cells whose value actually changed.
                    for cpid, _meal in payload:
                        self.boardview.ensure_pid(cpid)
                    self.boardview.render()
                elif kind == "roster":
                    self.boardview.set_order(self.store.known_pids())
                elif kind == "synced":
                    if payload:
                        self._set_sync(f"⇡ {payload} pending", WARN)
                    else:
                        self._set_sync("● all scans synced", GOOD)
                elif kind == "offline":
                    self._set_sync(f"⇢ offline — {payload} queued safely",
                                   BAD)
                elif kind == "online":
                    self._flash("Google Sheets connected — "
                                "sheet is source of truth.", GOOD)
                elif kind == "healed":
                    self._flash("Scan Log was wiped — headers/formulas "
                                "restored, board cleared to match.", WARN)
                elif kind == "offline-init":
                    self._set_sync("⇢ can't reach Google — scans queue "
                                   "locally, retrying…", WARN)
        except queue.Empty:
            pass
        except Exception:
            logger.exception("UI event pump error")
        self.after(100, self._drain_ui)


# --------------------------------------------------------------------------- #
#  Entry point — no network required to start; everything degrades gracefully.
# --------------------------------------------------------------------------- #
def main() -> None:
    setup_logging()
    logger.info("=== scanner starting: station=%s db=%s ===",
                MACHINE_ID, DB_FILE)

    dq: DurableQueue | MemoryQueue
    try:
        dq = DurableQueue()
    except Exception:
        logger.critical("SQLite completely unavailable — running with the "
                        "NON-DURABLE in-memory queue", exc_info=True)
        dq = MemoryQueue()

    recovered = import_emergency(dq)
    if recovered:
        logger.warning("recovered %d scan(s) from the emergency log",
                       recovered)

    store = Store(dq)
    store.bootstrap_local()        # provisional truth until the first poll

    app = App(store)
    mgr = SheetClientManager(app.ui_events)
    mgr.start()
    Flusher(store, mgr, app.ui_events).start()
    Poller(store, mgr, app.ui_events).start()
    store.flush_event.set()
    app.mainloop()
    logger.info("=== scanner exited: %d unsynced ===", store.unsynced_count())


if __name__ == "__main__":
    main()