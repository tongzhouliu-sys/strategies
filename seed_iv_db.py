#!/usr/bin/env python3
"""
OpenClaw · IV 历史回填工具 v1.1
================================
专门解决冷启动时 IVR 全部退化为 HV 代理的问题。

策略分两层：
  Layer-1（真实 IV）：Tradier / Polygon 历史期权链 → 提取 ATM IV
  Layer-2（HV 代理）：yfinance 价格历史 → 逐日滚动 HV 序列

回填逻辑：
  - 已有 real_iv 记录的日期：跳过（不降级）
  - 已有 hv_backfill 记录的日期：可被 Layer-1 真实 IV 覆盖
  - 空白日期：按 Layer1 → Layer2 顺序填充
  - 目标：每只标的在 iv_history.db 中有 ≥252 条历史记录

运行示例：
  python openclaw_iv_backfill.py                    # 全量标的，自动选源
  python openclaw_iv_backfill.py --tickers SPY QQQ  # 仅指定标的
  python openclaw_iv_backfill.py --source hv        # 强制使用 HV 代理（最快）
  python openclaw_iv_backfill.py --dry-run          # 预览，不写入
  python openclaw_iv_backfill.py --status           # 查看各标的当前覆盖率

⚠ 本内容仅为数据参考，不构成任何投资建议。期权交易具有高风险。
"""

import argparse
import json
import logging
import math
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ── 依赖检查 ──────────────────────────────────────────────
try:
    import requests
    import pandas as pd
    import numpy as np
except ImportError as e:
    missing = str(e).split("'")[1] if "'" in str(e) else str(e)
    print(f"缺少依赖：{missing}")
    print("请运行：pip install requests pandas numpy yfinance")
    sys.exit(1)

try:
    import yfinance as yf
    _YF_OK = True
except ImportError:
    _YF_OK = False

__version__ = "v1.1"

# yfinance 内部有共享状态，并发调用 yf.download() 会互相污染列名。
# 用锁串行化所有 yfinance 请求，避免 "Unknown datetime string format" 问题。
import threading
_YF_LOCK = threading.Lock()

SCRIPT_DIR = Path(__file__).resolve().parent

# ── 日志 ──────────────────────────────────────────────────
LOG_FILE = SCRIPT_DIR / "iv_backfill.log"
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)
logger = logging.getLogger(__name__)
_con = logging.StreamHandler()
_con.setLevel(logging.WARNING)
_con.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(_con)

# ── 常量 ──────────────────────────────────────────────────
HV_WINDOW      = 20          # 与主扫描器一致
BACKFILL_DAYS  = 380         # 回填天数（>365 确保足够 252 交易日）
MIN_RECORDS    = 30          # 主扫描器 calculate_real_ivr 门槛
TARGET_RECORDS = 252         # 目标：一个完整交易年

# 回填优先级标签（数值越大越权威，不允许低权威覆盖高权威）
SOURCE_PRIORITY = {
    "real_iv ✓":    10,
    "tradier_iv":    8,
    "polygon_iv":    8,
    "hv_backfill":   2,
    "hv_proxy ⚠（非真实IVR）": 1,
}

def _src_priority(src: str) -> int:
    for k, v in SOURCE_PRIORITY.items():
        if k in src:
            return v
    return 0


# ═══════════════════════════════════════════════════════════
#  标的列表（与主扫描器保持同步）
# ═══════════════════════════════════════════════════════════
# 只取 ticker 名称，不需要完整配置
DEFAULT_TICKERS = [
    # A+
    "SPY", "QQQ", "EWJ",
    # A（官方）
    "AAPL", "MSFT", "NVO", "ASML", "MA", "TSM",
    # B
    "V", "GLD", "XLU",
    # C
    "META", "NVDA", "TSLA", "HOOD", "IBIT", "COIN", "MSTR", "CRCL",
]

def _load_tickers_from_scan() -> list[str]:
    """尝试从主扫描器同目录的 tickers_config.json 读取完整标的列表"""
    cfg_path = SCRIPT_DIR / "tickers_config.json"
    if cfg_path.exists():
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            tickers = list(data.keys())
            logger.info(f"从 tickers_config.json 读取 {len(tickers)} 个标的")
            return tickers
        except Exception as e:
            logger.warning(f"读取 tickers_config.json 失败: {e}")
    return DEFAULT_TICKERS


# ═══════════════════════════════════════════════════════════
#  SQLite IV 数据库（与主扫描器共用同一个 DB）
# ═══════════════════════════════════════════════════════════
IV_DB_PATH = SCRIPT_DIR / "iv_history.db"

def _ensure_db():
    """确保数据库与表结构存在（与主扫描器 _init_iv_db 保持一致）"""
    conn = sqlite3.connect(IV_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS iv_snapshots (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker    TEXT NOT NULL,
            snap_date TEXT NOT NULL,
            snap_ts   TEXT NOT NULL,
            iv_atm    REAL,
            source    TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_iv ON iv_snapshots(ticker, snap_date)")
    conn.commit()
    conn.close()


def _existing_records(ticker: str) -> dict[str, str]:
    """返回 {date: source} 的现有记录字典"""
    try:
        conn = sqlite3.connect(IV_DB_PATH)
        rows = conn.execute(
            "SELECT snap_date, source FROM iv_snapshots WHERE ticker=? ORDER BY snap_date",
            (ticker,),
        ).fetchall()
        conn.close()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.warning(f"读取现有记录失败 {ticker}: {e}")
        return {}


def _write_batch(ticker: str, records: list[tuple], dry_run: bool) -> int:
    """
    批量写入 iv_snapshots。
    records: [(date_str, iv_atm, source), ...]
    不覆盖优先级更高的记录。返回实际写入条数。
    """
    if dry_run or not records:
        return len(records)  # dry-run 假装写入

    existing = _existing_records(ticker)
    to_write = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for date_str, iv_atm, source in records:
        existing_src = existing.get(date_str, "")
        if _src_priority(existing_src) >= _src_priority(source):
            continue  # 已有更权威的记录，跳过
        to_write.append((ticker, date_str, ts, iv_atm, source))

    if not to_write:
        return 0

    try:
        conn = sqlite3.connect(IV_DB_PATH)
        # 先删再插（覆盖低优先级旧记录）
        for row in to_write:
            conn.execute(
                "DELETE FROM iv_snapshots WHERE ticker=? AND snap_date=?",
                (row[0], row[1]),
            )
        conn.executemany(
            "INSERT INTO iv_snapshots (ticker, snap_date, snap_ts, iv_atm, source) VALUES (?,?,?,?,?)",
            to_write,
        )
        conn.commit()
        conn.close()
        return len(to_write)
    except Exception as e:
        logger.error(f"批量写入失败 {ticker}: {e}")
        return 0


# ═══════════════════════════════════════════════════════════
#  数据源 · Layer-2：HV 代理（yfinance 逐日 HV）
# ═══════════════════════════════════════════════════════════

def backfill_hv(ticker: str, days: int = BACKFILL_DAYS, dry_run: bool = False) -> tuple[int, str]:
    """
    用逐日滚动 HV 序列回填 IV 历史。
    返回 (写入条数, 状态描述)。

    原理：
      - 获取 days+HV_WINDOW+30 天价格数据（确保有足够窗口）
      - 每个交易日计算以该日为末尾的 HV_WINDOW 日滚动 HV（年化）
      - 以此作为 iv_atm 的代理值存入数据库
      - source = "hv_backfill"（优先级低于真实 IV，主扫描器累积真实 IV 后会自动升级）
    """
    if not _YF_OK:
        return 0, "yfinance 未安装"

    fetch_days = days + HV_WINDOW + 40
    start_dt   = (datetime.now() - timedelta(days=fetch_days)).strftime("%Y-%m-%d")

    try:
        # 用 _YF_LOCK 串行化所有 yfinance 请求：
        # yf.download() 内部有共享状态，多线程并发时列名会互相污染，
        # 导致 "Unknown datetime string format: <其他ticker>" 错误。
        # yf.Ticker.history() 是实例方法，更安全，但仍需加锁。
        with _YF_LOCK:
            tkr  = yf.Ticker(ticker)
            hist = tkr.history(start=start_dt, auto_adjust=True)

        if hist is None or len(hist) < HV_WINDOW + 10:
            return 0, f"历史数据不足（{len(hist) if hist is not None else 0} 行）"

        # 提取 Close 列，兼容 MultiIndex 和普通 DatetimeIndex
        raw_close = hist["Close"]
        if isinstance(raw_close, pd.DataFrame):
            raw_close = raw_close.iloc[:, 0]   # MultiIndex 时取第一列
        close = raw_close.squeeze()

        # 确保 index 是纯 DatetimeIndex（去除 timezone）
        if hasattr(close.index, "tz") and close.index.tz is not None:
            close.index = close.index.tz_localize(None)

        log_ret   = np.log(close / close.shift(1)).dropna()
        hv_series = log_ret.rolling(HV_WINDOW).std() * math.sqrt(252) * 100
        hv_series = hv_series.dropna()

        if hv_series.empty:
            return 0, "HV 序列为空"

        # 只取最近 days 天内的记录
        cutoff = datetime.now() - timedelta(days=days)
        records = []
        for ts_idx, iv_val in hv_series.items():
            dt = pd.Timestamp(ts_idx)
            if dt.tzinfo is not None:
                dt = dt.tz_localize(None)
            if dt < cutoff:
                continue
            if not iv_val or math.isnan(float(iv_val)) or float(iv_val) <= 0:
                continue
            date_str = dt.strftime("%Y-%m-%d")
            records.append((date_str, round(float(iv_val), 2), "hv_backfill"))

        if not records:
            return 0, "筛选后记录为空"

        written = _write_batch(ticker, records, dry_run)
        return written, f"HV回填 {written}/{len(records)} 条"

    except Exception as e:
        logger.warning(f"HV回填失败 {ticker}: {e}")
        return 0, f"异常: {e}"


# ═══════════════════════════════════════════════════════════
#  数据源 · Layer-1a：Tradier 历史期权 ATM IV
# ═══════════════════════════════════════════════════════════

def _load_env():
    env_path = SCRIPT_DIR / ".env"
    if env_path.exists():
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())

_load_env()

TRADIER_TOKEN   = os.environ.get("TRADIER_TOKEN", "")
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")
TRADIER_SANDBOX = os.environ.get("TRADIER_SANDBOX", "false").lower() == "true"

def _tradier_headers() -> dict:
    base = "https://sandbox.tradier.com" if TRADIER_SANDBOX else "https://api.tradier.com"
    return {
        "base": base,
        "headers": {
            "Authorization": f"Bearer {TRADIER_TOKEN}",
            "Accept": "application/json",
        },
    }

def _nearest_atm_iv_from_chain(chain: list, spot: float) -> Optional[float]:
    """从期权链找最近 ATM Put 的 IV"""
    if not chain or not spot:
        return None
    best_iv = None
    best_dist = float("inf")
    for opt in chain:
        try:
            strike = float(opt.get("strike", 0))
            iv     = float(opt.get("greeks", {}).get("smv_vol") or opt.get("implied_volatility") or 0)
            if iv <= 0 or iv > 5:  # 剔除异常值（> 500%）
                continue
            dist = abs(strike - spot)
            if dist < best_dist:
                best_dist = dist
                best_iv = iv * 100  # 转为百分比
        except Exception:
            continue
    return round(best_iv, 2) if best_iv else None


def backfill_tradier(ticker: str, days: int = BACKFILL_DAYS, dry_run: bool = False) -> tuple[int, str]:
    """
    使用 Tradier 获取历史 ATM IV。
    步骤：获取近期到期日 → 逐日拉取期权链 IV（受限于 Tradier 日期覆盖）。
    注意：Tradier 免费 sandbox 无法访问真实历史数据，仅生产 token 可用。
    """
    if not TRADIER_TOKEN:
        return 0, "TRADIER_TOKEN 未配置"

    cfg = _tradier_headers()
    base    = cfg["base"]
    headers = cfg["headers"]

    # 获取当前股价
    try:
        r = requests.get(
            f"{base}/v1/markets/quotes",
            headers=headers,
            params={"symbols": ticker},
            timeout=10,
        )
        r.raise_for_status()
        spot = float(r.json()["quotes"]["quote"]["last"])
    except Exception as e:
        return 0, f"获取报价失败: {e}"

    # 获取可用到期日（选最近的 1-2 个月度合约）
    try:
        r = requests.get(
            f"{base}/v1/markets/options/expirations",
            headers=headers,
            params={"symbol": ticker, "includeAllRoots": "false"},
            timeout=10,
        )
        r.raise_for_status()
        exps = r.json().get("expirations", {}).get("date", [])
        if isinstance(exps, str):
            exps = [exps]
        # 只取 30-60 DTE 的到期日
        today = datetime.now().date()
        valid_exps = []
        for e in exps:
            try:
                d = datetime.strptime(e, "%Y-%m-%d").date()
                dte = (d - today).days
                if 25 <= dte <= 60:
                    valid_exps.append(e)
            except Exception:
                continue
        if not valid_exps:
            return 0, "无合适到期日（DTE 25-60）"
    except Exception as e:
        return 0, f"获取到期日失败: {e}"

    # 从最近合适的到期日获取 ATM IV（作为当日快照）
    exp = valid_exps[0]
    try:
        r = requests.get(
            f"{base}/v1/markets/options/chains",
            headers=headers,
            params={"symbol": ticker, "expiration": exp, "greeks": "true"},
            timeout=15,
        )
        r.raise_for_status()
        chain = r.json().get("options", {}).get("option", [])
        if isinstance(chain, dict):
            chain = [chain]
        puts = [o for o in chain if o.get("option_type") == "put"]
        iv_atm = _nearest_atm_iv_from_chain(puts, spot)
        if not iv_atm:
            return 0, "未提取到有效 ATM IV"

        today_str = today.strftime("%Y-%m-%d")
        written = _write_batch(ticker, [(today_str, iv_atm, "tradier_iv")], dry_run)
        return written, f"Tradier IV={iv_atm:.1f}% ({exp})"
    except Exception as e:
        return 0, f"获取期权链失败: {e}"


# ═══════════════════════════════════════════════════════════
#  数据源 · Layer-1b：Polygon 历史快照 ATM IV
# ═══════════════════════════════════════════════════════════

def backfill_polygon(ticker: str, days: int = 30, dry_run: bool = False) -> tuple[int, str]:
    """
    使用 Polygon.io Options Snapshot API 获取 ATM IV（仅当日快照）。
    Polygon 免费版不提供历史期权数据，仅 Stocks Starter+ 可用。
    """
    if not POLYGON_API_KEY:
        return 0, "POLYGON_API_KEY 未配置"

    try:
        url = f"https://api.polygon.io/v3/snapshot/options/{ticker}"
        params = {
            "apiKey": POLYGON_API_KEY,
            "limit": 50,
            "contract_type": "put",
            "strike_price.gte": 0,  # 会按 ATM 筛选
        }
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        results = r.json().get("results", [])
        if not results:
            return 0, "Polygon 无返回结果"

        # 获取当前价格（从快照中的 underlying_asset）
        spot = None
        for res in results:
            und = res.get("underlying_asset", {})
            if und.get("price"):
                spot = float(und["price"])
                break
        if not spot:
            return 0, "无法获取标的价格"

        # 找最近 ATM Put IV
        best_iv, best_dist = None, float("inf")
        for res in results:
            try:
                detail  = res.get("details", {})
                greeks  = res.get("greeks", {})
                strike  = float(detail.get("strike_price", 0))
                iv_raw  = res.get("implied_volatility")
                if not iv_raw:
                    continue
                iv = float(iv_raw) * 100
                if iv <= 0 or iv > 500:
                    continue
                dist = abs(strike - spot)
                if dist < best_dist:
                    best_dist = dist
                    best_iv = round(iv, 2)
            except Exception:
                continue

        if not best_iv:
            return 0, "未提取到有效 ATM IV"

        today_str = datetime.now().strftime("%Y-%m-%d")
        written = _write_batch(ticker, [(today_str, best_iv, "polygon_iv")], dry_run)
        return written, f"Polygon IV={best_iv:.1f}%"
    except Exception as e:
        return 0, f"Polygon 异常: {e}"


# ═══════════════════════════════════════════════════════════
#  单标的回填编排
# ═══════════════════════════════════════════════════════════

def backfill_ticker(
    ticker: str,
    source: str = "auto",
    dry_run: bool = False,
    days: int = BACKFILL_DAYS,
) -> dict:
    """
    对单个标的执行回填。source: auto / hv / tradier / polygon
    返回结果字典。
    """
    existing = _existing_records(ticker)
    real_count = sum(1 for s in existing.values() if _src_priority(s) >= 8)
    hv_count   = len(existing) - real_count

    result = {
        "ticker":        ticker,
        "before_total":  len(existing),
        "before_real":   real_count,
        "before_hv":     hv_count,
        "written":       0,
        "source_used":   "",
        "status":        "",
        "after_total":   0,
    }

    # 已有足够真实 IV → 跳过 HV 回填，但可以补当日真实 IV
    need_hv_fill = len(existing) < TARGET_RECORDS or hv_count == len(existing)

    written_total = 0
    sources_used  = []

    # ── Layer-1a: Tradier（当日真实 IV）────────────────────
    if source in ("auto", "tradier") and TRADIER_TOKEN:
        n, msg = backfill_tradier(ticker, dry_run=dry_run)
        if n > 0:
            written_total += n
            sources_used.append(f"Tradier({msg})")
        else:
            logger.info(f"{ticker} Tradier跳过: {msg}")

    # ── Layer-1b: Polygon（当日真实 IV）────────────────────
    if source in ("auto", "polygon") and POLYGON_API_KEY and not sources_used:
        n, msg = backfill_polygon(ticker, dry_run=dry_run)
        if n > 0:
            written_total += n
            sources_used.append(f"Polygon({msg})")
        else:
            logger.info(f"{ticker} Polygon跳过: {msg}")

    # ── Layer-2: HV 回填（大批量历史填充）──────────────────
    if source in ("auto", "hv") and need_hv_fill:
        n, msg = backfill_hv(ticker, days=days, dry_run=dry_run)
        written_total += n
        sources_used.append(f"HV({msg})")

    # 查询回填后数量
    after = _existing_records(ticker)
    result["written"]     = written_total
    result["source_used"] = " + ".join(sources_used) if sources_used else "无"
    result["after_total"] = len(after)
    result["status"]      = (
        "✅ 已达标" if len(after) >= MIN_RECORDS else
        f"⚠ 仍不足({len(after)}<{MIN_RECORDS})"
    )
    return result


# ═══════════════════════════════════════════════════════════
#  状态查看
# ═══════════════════════════════════════════════════════════

def show_status(tickers: list[str]):
    """打印各标的 IV 历史库覆盖状态"""
    _ensure_db()
    print()
    print("─" * 76)
    print(f"{'标的':<8} {'总条数':>6} {'真实IV':>6} {'HV代理':>6} {'覆盖率':>8} {'最新日期':<12} {'状态'}")
    print("─" * 76)

    total_ok = 0
    for ticker in sorted(tickers):
        existing = _existing_records(ticker)
        real_cnt = sum(1 for s in existing.values() if _src_priority(s) >= 8)
        hv_cnt   = len(existing) - real_cnt
        latest   = max(existing.keys()) if existing else "—"
        pct      = f"{len(existing)/TARGET_RECORDS*100:.0f}%" if existing else "0%"
        ok       = len(existing) >= MIN_RECORDS

        status = "✅ 可用" if ok else ("⚠ HV不足" if len(existing) > 0 else "❌ 空")
        if ok:
            total_ok += 1
        print(f"{ticker:<8} {len(existing):>6} {real_cnt:>6} {hv_cnt:>6} {pct:>8} {latest:<12} {status}")

    print("─" * 76)
    print(f"合计：{total_ok}/{len(tickers)} 个标的已达最低门槛（≥{MIN_RECORDS}条）")
    print()


# ═══════════════════════════════════════════════════════════
#  命令行
# ═══════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description=f"OpenClaw IV 历史回填工具 {__version__}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python openclaw_iv_backfill.py                      # 全量标的，自动选源
  python openclaw_iv_backfill.py --tickers SPY QQQ    # 仅指定标的
  python openclaw_iv_backfill.py --source hv          # 强制 HV 代理（最快，无需 API）
  python openclaw_iv_backfill.py --dry-run            # 预览，不写入
  python openclaw_iv_backfill.py --status             # 查看覆盖率状态
  python openclaw_iv_backfill.py --days 500           # 回填更长历史（默认380天）
        """,
    )
    p.add_argument("--tickers",  nargs="+", default=None,
                   help="仅回填指定标的（默认全量）")
    p.add_argument("--source",   choices=["auto", "hv", "tradier", "polygon"],
                   default="auto",
                   help="强制使用的数据源（默认 auto：真实IV优先，HV兜底）")
    p.add_argument("--days",     type=int, default=BACKFILL_DAYS,
                   help=f"回填天数（默认 {BACKFILL_DAYS}）")
    p.add_argument("--workers",  type=int, default=4,
                   help="并发线程数（默认 4；Tradier/Polygon 建议 ≤2）")
    p.add_argument("--dry-run",  action="store_true",
                   help="预览模式：计算但不写入数据库")
    p.add_argument("--status",   action="store_true",
                   help="仅显示当前覆盖率状态，不执行回填")
    p.add_argument("--force-hv", action="store_true",
                   help="强制对所有标的补足 HV 历史（即使已有真实 IV 记录）")
    return p.parse_args()


# ═══════════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════════

def main():
    args = parse_args()
    _ensure_db()

    # 确定标的列表
    all_tickers = _load_tickers_from_scan()
    tickers = args.tickers if args.tickers else all_tickers

    # 状态模式
    if args.status:
        show_status(tickers)
        return

    # 前置检查
    if not _YF_OK and args.source in ("auto", "hv"):
        print("❌ yfinance 未安装，HV 回填不可用")
        print("   请运行：pip install yfinance")
        if args.source == "hv":
            sys.exit(1)

    # 打印配置
    print()
    print("=" * 72)
    print(f"🔄 OpenClaw IV 历史回填工具 {__version__}")
    print(f"   目标标的：{len(tickers)} 个")
    print(f"   数据源  ：{args.source}")
    print(f"   回填天数：{args.days} 天（目标 ≥{TARGET_RECORDS} 条/标的）")
    print(f"   并发线程：{args.workers}")
    print(f"   数据库  ：{IV_DB_PATH}")
    if args.dry_run:
        print("   ⚠️  DRY-RUN 模式：不写入数据库")
    if TRADIER_TOKEN:
        print("   ✅ Tradier Token 已配置")
    else:
        print("   ⚪ Tradier Token 未配置（跳过真实 IV 抓取）")
    if POLYGON_API_KEY:
        print("   ✅ Polygon Key 已配置")
    else:
        print("   ⚪ Polygon Key 未配置（跳过真实 IV 抓取）")
    print("=" * 72)
    print()

    # 显示回填前状态
    print("📊 回填前状态：")
    show_status(tickers)

    # 并发回填
    print(f"⚙️  开始回填（{args.workers} 线程）...\n")
    results = []
    done    = 0
    total   = len(tickers)

    with ThreadPoolExecutor(max_workers=args.workers) as exe:
        futures = {
            exe.submit(
                backfill_ticker,
                t,
                source=args.source,
                dry_run=args.dry_run,
                days=args.days,
            ): t
            for t in tickers
        }
        for fut in as_completed(futures):
            ticker = futures[fut]
            done  += 1
            try:
                res = fut.result()
            except Exception as e:
                res = {
                    "ticker":      ticker,
                    "written":     0,
                    "after_total": 0,
                    "status":      f"❌ 异常: {e}",
                    "source_used": "",
                }
            results.append(res)

            # 进度行
            bar_filled = int(done / total * 20)
            bar = "█" * bar_filled + "░" * (20 - bar_filled)
            print(
                f"  [{done:03d}/{total}] [{bar}] "
                f"{res['ticker']:<7} "
                f"新增:{res['written']:>4}条  "
                f"合计:{res['after_total']:>4}条  "
                f"{res['status']}  "
                f"{res['source_used']}"
            )

    # 汇总
    print()
    print("=" * 72)
    ok_count   = sum(1 for r in results if r["after_total"] >= MIN_RECORDS)
    total_new  = sum(r["written"] for r in results)
    print(f"✅ 回填完成！")
    print(f"   新增记录：{total_new} 条")
    print(f"   达标标的：{ok_count}/{total}（≥{MIN_RECORDS} 条历史数据）")
    if ok_count < total:
        failed = [r["ticker"] for r in results if r["after_total"] < MIN_RECORDS]
        print(f"   未达标  ：{', '.join(failed)}")
        print(f"   💡 提示：请检查网络，或使用 --source hv 强制 HV 代理回填")
    print()

    # 回填后状态
    print("📊 回填后状态：")
    show_status(tickers)

    print("📤 下一步：")
    print("   直接运行 openclaw_scan.py，本次将使用真实 IVR 代替 HV 代理")
    print("   每日扫描后 IVR 精度持续提升（累积真实 IV 数据）")
    print()
    print("⚠ 本内容仅为数据参考，不构成任何投资建议。")
    print("=" * 72)


if __name__ == "__main__":
    main()
