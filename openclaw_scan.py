#!/usr/bin/env python3
"""
OpenClaw · Wheel卖Put · 期权链扫描器 v3.1
==========================================

功能概要：Put 期权链扫描、Pre-screen Gate、宏观事件窗口（FOMC/CPI/NFP/BOJ）、
全量标的扫描、LLM 精简 JSON 输出。

安装：pip install requests yfinance pandas numpy

配置（推荐脚本同目录 .env；缺省含开发用 Massive / Alpaca Paper 占位，生产请改用私有密钥）：
  MASSIVE_KEYS / MASSIVE_BASE_URL
  ALPACA_PAPER_KEY / ALPACA_PAPER_SECRET
  TRADIER_TOKEN / POLYGON_API_KEY

运行示例：
  python openclaw_scan.py
  python openclaw_scan.py
  python openclaw_scan.py --block-hv-proxy --block-rising-iv
  python openclaw_scan.py --margin-used 42.5 --save-raw-json

⚠ 本内容仅为数据参考，不构成任何投资建议。期权交易具有高风险。
"""

import json
import sys
import math
import time
import sqlite3
import logging
import argparse
import os
import re
import html
import threading
import socket
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Optional
from collections import defaultdict

__version__ = "v3.1"

# 扫描结果（LLM/摘要/raw JSON）始终写入脚本所在目录，与当前工作目录无关
SCRIPT_DIR = Path(__file__).resolve().parent

# 与本脚本写出文件名一致：LLM_YYYYMMDD_HHMM.txt 或带 _compact / _full 后缀
_LLM_OUTPUT_TXT_RE = re.compile(r"^LLM_\d{8}_\d{4}(?:_(?:compact|full))?\.txt$")

# ── 依赖检查 ────────────────────────────────────────────────
try:
    import requests
    import pandas as pd
    import numpy as np
except ImportError as e:
    missing = str(e).split("'")[1] if "'" in str(e) else str(e)
    print(f"缺少依赖：{missing}")
    print("请运行：pip install requests pandas numpy")
    sys.exit(1)

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False


# ═══════════════════════════════════════════════════════════
#  日志系统
# ═══════════════════════════════════════════════════════════

LOG_FILE = SCRIPT_DIR / "scanner.log"
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)
logger = logging.getLogger(__name__)

_console = logging.StreamHandler()
_console.setLevel(logging.WARNING)
_console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(_console)


# ═══════════════════════════════════════════════════════════
#  命令行参数
# ═══════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description=f"OpenClaw {__version__} · Wheel卖Put期权链扫描器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source",      choices=["auto","massive","alpaca","tradier","polygon","yfinance"],
                        default="auto",  help="数据源（默认auto）")
    parser.add_argument("--dte-min",     type=int,   default=35)
    parser.add_argument("--dte-max",     type=int,   default=45)
    parser.add_argument("--dte-fb-min",  type=int,   default=30)
    parser.add_argument("--dte-fb-max",  type=int,   default=60)
    parser.add_argument("--delta-min",   type=float, default=-0.30)
    parser.add_argument("--delta-max",   type=float, default=-0.15)
    parser.add_argument("--risk-free",   type=float, default=0.045)
    parser.add_argument("--workers",     type=int,   default=None)
    parser.add_argument("--batch-size",  type=int,   default=0,
                        help="分批扫描标的数量（0=不分批；建议20~30）")
    parser.add_argument("--keep-files",  type=int,   default=200)
    parser.add_argument("--max-spread",  type=float, default=15.0,
                        help="bid/ask价差上限%%（默认15）")
    parser.add_argument("--min-oi",      type=int,   default=50,
                        help="最低持仓量OI（默认50）")
    parser.add_argument("--margin-used", type=float, default=None,
                        help="当前保证金使用率%%")
    parser.add_argument("--massive-gap", type=float, default=20.0,
                        help="Massive API 单密钥最小调用间隔秒数（默认20）")
    parser.add_argument("--save-raw-json",   action="store_true",
                        help="同时保存完整原始JSON（默认关闭）")
    parser.add_argument("--compact-output",  action="store_true",
                        help="极简LLM输出（最大限度压缩token，跳过标的仅统计数量）")
    parser.add_argument("--full-output",     action="store_true",
                        help="完整LLM输出（包含所有合约细节，适合调试）")
    parser.add_argument("--block-hv-proxy",  action="store_true",
                        help="IVR为HV代理时不产生信号（默认允许但标记警告）")
    parser.add_argument("--block-rising-iv", action="store_true",
                        help="IV趋势上升时不产生信号（默认允许但标记警告）")
    parser.add_argument("--block-post-earnings", action="store_true",
                        help="财报后vol crush窗口内不产生信号（默认只warn不block）")
    parser.add_argument("--with-legend", dest="with_legend", action="store_true",
                        help="LLM JSON附带字段图例（默认关闭，可按需开启）")
    parser.add_argument("--no-legend", dest="with_legend", action="store_false",
                        help="关闭LLM JSON字段图例（进一步节省token）")
    parser.add_argument("--pretty-json", action="store_true",
                        help="LLM JSON使用缩进格式输出（默认紧凑以节省token）")
    parser.add_argument("--disable-cboe", action="store_true",
                        help="禁用 CBOE 期权源，仅使用其他数据源")
    parser.add_argument("--cboe-gap", type=float, default=1.0,
                        help="CBOE 请求最小间隔秒数（默认1.0）")
    parser.set_defaults(with_legend=False)
    return parser.parse_args()


def _default_args() -> argparse.Namespace:
    """导入模块时使用默认参数，避免立即解析CLI参数。"""
    return argparse.Namespace(
        source="auto",
        dte_min=35,
        dte_max=45,
        dte_fb_min=30,
        dte_fb_max=60,
        delta_min=-0.30,
        delta_max=-0.15,
        risk_free=0.045,
        workers=None,
        batch_size=0,
        keep_files=200,
        max_spread=15.0,
        min_oi=50,
        margin_used=None,
        massive_gap=20.0,
        save_raw_json=False,
        compact_output=False,
        full_output=False,
        block_hv_proxy=False,
        block_rising_iv=False,
        block_post_earnings=False,
        with_legend=False,
        pretty_json=False,
        disable_cboe=False,
        cboe_gap=1.0,
    )


ARGS = _default_args()

RISK_FREE_RATE    = ARGS.risk_free
DELTA_MIN         = ARGS.delta_min
DELTA_MAX         = ARGS.delta_max
DTE_PREFERRED_MIN = ARGS.dte_min
DTE_PREFERRED_MAX = ARGS.dte_max
DTE_FALLBACK_MIN  = ARGS.dte_fb_min
DTE_FALLBACK_MAX  = ARGS.dte_fb_max
KEEP_FILES        = ARGS.keep_files
MAX_SPREAD_PCT    = ARGS.max_spread
MIN_OI            = ARGS.min_oi
IVR_HISTORY_PERIOD = "1y"
HV_WINDOW          = 20
NEAR_BLACKOUT_BUFFER = 7
POST_EARNINGS_VOL_DAYS = 5

# FOMC 日历（硬编码，可按年维护）
FOMC_DATES = [
    "2025-12-17",
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
    "2027-01-27", "2027-03-17",
]

# 美国 CPI（BLS CPI 发布日程；按年核对 bls.gov/cpi/）
CPI_DATES = [
    "2026-01-13", "2026-02-11", "2026-03-11", "2026-04-10", "2026-05-12",
    "2026-06-10", "2026-07-14", "2026-08-12", "2026-09-11", "2026-10-14",
    "2026-11-10", "2026-12-10",
    "2027-01-13", "2027-02-10", "2027-03-10",
]

# 美国非农就业（BLS empsit；遇联邦假日等会调整发布日，见 bls.gov/schedule/news_release/empsit.htm）
NFP_DATES = [
    "2026-01-09", "2026-02-11", "2026-03-06", "2026-04-03", "2026-05-08",
    "2026-06-05", "2026-07-02", "2026-08-07", "2026-09-04", "2026-10-02",
    "2026-11-06", "2026-12-04",
    "2027-01-08", "2027-02-05", "2027-03-05",
]

# 日本央行 MPM 决议次日（日银日程 boj.or.jp/en/mopo/mpmsche_minu/）
BOJ_DATES = [
    "2026-01-23", "2026-03-19", "2026-04-28", "2026-06-16", "2026-07-31",
    "2026-09-18", "2026-10-30", "2026-12-18",
    "2027-01-22",
]


def _days_to_next_macro(date_list: list, today=None) -> Optional[int]:
    """距 date_list 中下一事件日的天数（含当日为 0）；无未来日期则 None。"""
    base = today or datetime.now().date()
    diffs: list[int] = []
    for ds in date_list:
        try:
            d = datetime.strptime(ds, "%Y-%m-%d").date()
        except Exception:
            continue
        diff = (d - base).days
        if diff >= 0:
            diffs.append(diff)
    return min(diffs) if diffs else None


def _days_to_next_fomc(today=None) -> Optional[int]:
    return _days_to_next_macro(FOMC_DATES, today)


def _days_to_next_cpi(today=None) -> Optional[int]:
    return _days_to_next_macro(CPI_DATES, today)


def _days_to_next_nfp(today=None) -> Optional[int]:
    return _days_to_next_macro(NFP_DATES, today)


def _days_to_next_boj(today=None) -> Optional[int]:
    return _days_to_next_macro(BOJ_DATES, today)


# ── API Key 加载（.env 文件或环境变量）──────────────────────

def _load_env():
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())

_load_env()

# Massive API（.env 优先；无 MASSIVE_KEYS 时使用内置开发列表）
_massive_keys_raw  = os.environ.get("MASSIVE_KEYS", "")
MASSIVE_KEYS_LIST  = [k.strip() for k in _massive_keys_raw.split(",") if k.strip()]
_HARDCODED_MASSIVE_KEYS = [
    "eU0Zpp3efjJXmTQODfBWqLeDLenu661A",
    "AsbhzDqQSfOu0XFKyJ5PQv9hTwt3Gp6w",
    "n_msEg4lr8LJdKj_VoYSKkIqpbu7iXaN",
    "afbdVK0P8DFPy9tFmbA2WPY0CqwmO46e",
    "PORypD_lYqpAvp60ktiq7iST7RfFuTEn",
]
# 注意：以下默认密钥仅用于本地开发/演示，生产环境请务必使用私有 .env 覆盖。
if not MASSIVE_KEYS_LIST:
    MASSIVE_KEYS_LIST = _HARDCODED_MASSIVE_KEYS

MASSIVE_BASE_URL = os.environ.get(
    "MASSIVE_BASE_URL",
    "https://api.massivetrader.com/v1",
)

MASSIVE_ENDPOINTS = {
    "quote":        "quotes",
    "expirations":  "options/expirations",
    "chain":        "options/chain",
    "history":      "historical/bars",
}

# 其他 API Keys
TRADIER_TOKEN   = os.environ.get("TRADIER_TOKEN",   "")
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")
TRADIER_SANDBOX = os.environ.get("TRADIER_SANDBOX", "false").lower() == "true"

# Alpaca Paper API（环境变量优先，缺省为内置 Paper 占位）
ALPACA_PAPER_KEY = os.environ.get(
    "ALPACA_PAPER_KEY",
    "PK3CE4AJHF3IFMR5TCKIVW7ZPY",
)
ALPACA_PAPER_SECRET = os.environ.get(
    "ALPACA_PAPER_SECRET",
    "67bCoui27vXiYrrQRfSMB2k2WKjC1wHPd3xXpc5iNTHz",
)
ALPACA_PAPER_BASE_URL = os.environ.get(
    "ALPACA_PAPER_BASE_URL",
    "https://paper-api.alpaca.markets/v2",
)
ALPACA_DATA_BASE_URL = os.environ.get(
    "ALPACA_DATA_BASE_URL",
    "https://data.alpaca.markets",
)


# ═══════════════════════════════════════════════════════════
#  标的配置
# ═══════════════════════════════════════════════════════════
# 内置 fallback（无 tickers_config.json 时）：宏观黑名单仅 SPY/QQQ（FOMC+CPI+NFP）、
# EWJ（BOJ）、XLU（仅 FOMC）；其余标的仅财报等逻辑，宏观窗口为 0。生产以 JSON 为准。

_DEFAULT_TICKERS = {'SPY': {'grade': 'A+',
         'sector': 'ETF宽基',
         'ivr_min': 25,
         'ann_min': 10,
         'otm_buffer': 0.08,
         'earnings_blackout': 0,
         'fomc_blackout': 3,
         'cpi_blackout': 3,
         'nfp_blackout': 3,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': True,
         'blackout_desc': 'FOMC/CPI/非农前3天',
         'notes': 'SPY+QQQ合计≤净值25%；不同时满仓'},
 'QQQ': {'grade': 'A+',
         'sector': 'ETF宽基',
         'ivr_min': 28,
         'ann_min': 10,
         'otm_buffer': 0.08,
         'earnings_blackout': 0,
         'fomc_blackout': 3,
         'cpi_blackout': 3,
         'nfp_blackout': 3,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': True,
         'blackout_desc': 'FOMC/CPI/非农前3天',
         'notes': '与SPY不同时满仓；NVDA/AAPL财报周仓位×0.7'},
 'EWJ': {'grade': 'A+',
         'sector': 'ETF日本',
         'ivr_min': 30,
         'ann_min': 8,
         'otm_buffer': 0.08,
         'earnings_blackout': 0,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 2,
         'structure': 'CSP',
         'cc_rule': '暂停',
         'is_official': True,
         'blackout_desc': 'BOJ前后2天',
         'notes': '净值≥$500K；USD/JPY周变<3%；C状态直接暂停'},
 'IWM': {'grade': 'A+',
         'sector': 'ETF宽基',
         'ivr_min': 30,
         'ann_min': 10,
         'otm_buffer': 0.08,
         'earnings_blackout': 0,
         'fomc_blackout': 3,
         'cpi_blackout': 3,
         'nfp_blackout': 3,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': 'FOMC/CPI/非农前3天',
         'notes': '小盘风险偏好指标；与SPY高相关，注意集中度'},
 'XLK': {'grade': 'A+',
         'sector': 'ETF科技',
         'ivr_min': 28,
         'ann_min': 10,
         'otm_buffer': 0.08,
         'earnings_blackout': 0,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '无（ETF）',
         'notes': 'NVDA/AAPL权重高；财报周注意组合重叠'},
 'XLF': {'grade': 'A+',
         'sector': 'ETF金融',
         'ivr_min': 28,
         'ann_min': 10,
         'otm_buffer': 0.08,
         'earnings_blackout': 0,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '无（ETF）',
         'notes': '利率高度联动；FOMC前谨慎；区域银行事件敏感'},
 'XLV': {'grade': 'A+',
         'sector': 'ETF医疗',
         'ivr_min': 25,
         'ann_min': 9,
         'otm_buffer': 0.08,
         'earnings_blackout': 0,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '无（ETF）',
         'notes': '防御属性强；IV偏低需等待窗口；与SPX低相关'},
 'XLY': {'grade': 'A+',
         'sector': 'ETF消费',
         'ivr_min': 28,
         'ann_min': 10,
         'otm_buffer': 0.08,
         'earnings_blackout': 0,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '无（ETF）',
         'notes': 'AMZN/TSLA权重高；消费信心联动'},
 'AAPL': {'grade': 'A',
          'sector': '科技',
          'ivr_min': 38,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': True,
          'blackout_desc': '财报前10天',
          'notes': '被行权3日内推CC；接货超$60K需监控遗产税'},
 'MSFT': {'grade': 'A',
          'sector': '科技',
          'ivr_min': 35,
          'ann_min': 11,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': True,
          'blackout_desc': '财报前10天',
          'notes': 'IV曲线平滑；Azure业绩日注意但不自动停开'},
 'NVO': {'grade': 'A',
         'sector': '生物制药',
         'ivr_min': 40,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP',
         'cc_rule': '—',
         'is_official': True,
         'blackout_desc': '财报/FDA前10天',
         'notes': 'FDA事件视同财报；与SPX相关性<0.45，分散价值强'},
 'ASML': {'grade': 'A',
          'sector': '半导体设备',
          'ivr_min': 40,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP',
          'cc_rule': '暂停',
          'is_official': True,
          'blackout_desc': '财报前10天',
          'notes': '净值≥$1M；出口管制新闻立即暂停；价差/权利金<4%'},
 'MA': {'grade': 'A',
        'sector': '金融',
        'ivr_min': 32,
        'ann_min': 10,
        'otm_buffer': 0.06,
        'earnings_blackout': 10,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': 'CSP / C→价差',
        'cc_rule': '价差',
        'is_official': True,
        'blackout_desc': '财报前10天',
        'notes': 'MA+V合计≤净值12%；两者不同时超80%'},
 'TSM': {'grade': 'A',
         'sector': '半导体',
         'ivr_min': 42,
         'ann_min': 13,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP',
         'cc_rule': '暂停',
         'is_official': True,
         'blackout_desc': '财报前10天',
         'notes': '地缘监控（台海/chip export）；ADR换算比验证'},
 'GOOGL': {'grade': 'A',
           'sector': '科技',
           'ivr_min': 35,
           'ann_min': 11,
           'otm_buffer': 0.06,
           'earnings_blackout': 10,
           'fomc_blackout': 0,
           'cpi_blackout': 0,
           'nfp_blackout': 0,
           'boj_blackout': 0,
           'structure': 'CSP / C→价差',
           'cc_rule': '价差',
           'is_official': False,
           'blackout_desc': '财报前10天',
           'notes': '广告收入驱动；与GOOG共财报，二选一开仓'},
 'GOOG': {'grade': 'A',
          'sector': '科技',
          'ivr_min': 35,
          'ann_min': 11,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '与GOOGL共财报；不同时持有，二选一'},
 'AMZN': {'grade': 'A',
          'sector': '科技/零售',
          'ivr_min': 38,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': 'AWS+广告双驱动；AWS增速为核心跟踪指标'},
 'AVGO': {'grade': 'A',
          'sector': '半导体',
          'ivr_min': 42,
          'ann_min': 13,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': 'AI定制芯片敞口大；与NVDA高相关，注意组合集中度'},
 'ORCL': {'grade': 'A',
          'sector': '科技',
          'ivr_min': 38,
          'ann_min': 11,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '云数据库转型；财报RPO指引波动较大'},
 'CRM': {'grade': 'A',
         'sector': '科技SaaS',
         'ivr_min': 40,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': 'SaaS订阅模式；AI Agent商业化进展监控'},
 'ADBE': {'grade': 'A',
          'sector': '科技SaaS',
          'ivr_min': 40,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '创意软件护城河；AI替代风险与Firefly商业化'},
 'QCOM': {'grade': 'A',
          'sector': '半导体',
          'ivr_min': 40,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '手机终端敞口；中国市场依赖高，出口管制风险'},
 'TXN': {'grade': 'A',
         'sector': '半导体',
         'ivr_min': 35,
         'ann_min': 11,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '工业/汽车模拟芯片；强周期性；库存去化节奏'},
 'AMAT': {'grade': 'A',
          'sector': '半导体设备',
          'ivr_min': 42,
          'ann_min': 13,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '出口管制监控；与LRCX/KLAC高相关，注意集中度'},
 'LRCX': {'grade': 'A',
          'sector': '半导体设备',
          'ivr_min': 42,
          'ann_min': 13,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '出口管制监控；与AMAT/KLAC高相关，二选一'},
 'KLAC': {'grade': 'A',
          'sector': '半导体设备',
          'ivr_min': 42,
          'ann_min': 13,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '工艺控制设备垄断；与AMAT相关性高，注意集中'},
 'ANET': {'grade': 'A',
          'sector': '网络设备',
          'ivr_min': 45,
          'ann_min': 13,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': 'AI超算网络交换机龙头；云厂商资本支出联动'},
 'INTU': {'grade': 'A',
          'sector': '科技/金融',
          'ivr_min': 38,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '税务SaaS季节性强（1-4月IV升高）；AI竞争监控'},
 'NOW': {'grade': 'A',
         'sector': '科技SaaS',
         'ivr_min': 45,
         'ann_min': 13,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '企业IT平台龙头；AI Agent工作流商业化进展'},
 'JPM': {'grade': 'A',
         'sector': '银行',
         'ivr_min': 30,
         'ann_min': 10,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '美联储利率决议联动；与XLF/KRE高相关'},
 'GS': {'grade': 'A',
        'sector': '投行',
        'ivr_min': 35,
        'ann_min': 11,
        'otm_buffer': 0.06,
        'earnings_blackout': 10,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': 'CSP / C→价差',
        'cc_rule': '价差',
        'is_official': False,
        'blackout_desc': '财报前10天',
        'notes': '投行业务周期性强；并购/IPO市场活跃度联动'},
 'MS': {'grade': 'A',
        'sector': '投行',
        'ivr_min': 35,
        'ann_min': 11,
        'otm_buffer': 0.06,
        'earnings_blackout': 10,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': 'CSP / C→价差',
        'cc_rule': '价差',
        'is_official': False,
        'blackout_desc': '财报前10天',
        'notes': '财富管理+投行双驱动；AUM与市场正相关'},
 'BLK': {'grade': 'A',
         'sector': '资管',
         'ivr_min': 32,
         'ann_min': 10,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': 'AUM与市场正相关；VIX高时资产缩水敏感'},
 'SCHW': {'grade': 'A',
          'sector': '券商',
          'ivr_min': 38,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '利率敏感型；存款流失/货币基金转移风险监控'},
 'AXP': {'grade': 'A',
         'sector': '金融',
         'ivr_min': 32,
         'ann_min': 10,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '高端消费信用卡；消费景气度直接联动'},
 'WFC': {'grade': 'A',
         'sector': '银行',
         'ivr_min': 32,
         'ann_min': 10,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '资产上限松动进展；监管事件敏感度高'},
 'KKR': {'grade': 'A',
         'sector': '另类资管',
         'ivr_min': 40,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': 'PE+信贷双线；市场B/C状态时主动收紧仓位'},
 'APO': {'grade': 'A',
         'sector': '另类资管',
         'ivr_min': 40,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '信贷资管龙头；利率/信用利差高度敏感'},
 'BX': {'grade': 'A',
        'sector': '另类资管',
        'ivr_min': 40,
        'ann_min': 12,
        'otm_buffer': 0.06,
        'earnings_blackout': 10,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': 'CSP / C→价差',
        'cc_rule': '价差',
        'is_official': False,
        'blackout_desc': '财报前10天',
        'notes': '房地产+私募敞口；流动性事件与赎回风险监控'},
 'DIS': {'grade': 'A',
         'sector': '娱乐/媒体',
         'ivr_min': 40,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '流媒体扭亏+主题公园双驱动；Disney+订阅增速'},
 'NXPI': {'grade': 'A',
          'sector': '半导体',
          'ivr_min': 45,
          'ann_min': 13,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '汽车半导体龙头；中国市场依赖度高，出口管制风险'},
 'KRE': {'grade': 'A',
         'sector': 'ETF区域银行',
         'ivr_min': 38,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 0,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '无（ETF）',
         'notes': '区域银行危机风险监控；与利率/存款外流高度相关'},
 'LLY': {'grade': 'A',
         'sector': '生物制药',
         'ivr_min': 45,
         'ann_min': 13,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报/FDA前10天',
         'notes': 'GLP-1与NVO同赛道竞争；FDA关键数据视同财报'},
 'ISRG': {'grade': 'A',
          'sector': '医疗器械',
          'ivr_min': 40,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '达芬奇手术机器人垄断；装机量+手术量双跟踪'},
 'REGN': {'grade': 'A',
          'sector': '生物制药',
          'ivr_min': 45,
          'ann_min': 13,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报/FDA前10天',
          'notes': 'FDA临床数据高敏感；Dupixent适应证增速核心'},
 'VRTX': {'grade': 'A',
          'sector': '生物制药',
          'ivr_min': 45,
          'ann_min': 13,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报/FDA前10天',
          'notes': 'CF垄断+镇痛新药pipeline；FDA里程碑监控'},
 'BSX': {'grade': 'A',
         'sector': '医疗器械',
         'ivr_min': 35,
         'ann_min': 11,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '心脏介入器械；FDA审批敏感度低于biotech'},
 'HCA': {'grade': 'A',
         'sector': '医院',
         'ivr_min': 38,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '医院运营；医保报销政策（Medicaid）联动'},
 'UNH': {'grade': 'A',
         'sector': '医保',
         'ivr_min': 35,
         'ann_min': 10,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': 'Medicare Advantage政策风险；医疗损失率监控'},
 'ELV': {'grade': 'A',
         'sector': '医保',
         'ivr_min': 35,
         'ann_min': 11,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '与UNH高度相关；医保政策同步敏感，不同时超配'},
 'CI': {'grade': 'A',
        'sector': '医保',
        'ivr_min': 35,
        'ann_min': 11,
        'otm_buffer': 0.06,
        'earnings_blackout': 10,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': 'CSP / C→价差',
        'cc_rule': '价差',
        'is_official': False,
        'blackout_desc': '财报前10天',
        'notes': 'PBM业务监管风险；与UNH/ELV不同时满仓'},
 'SYK': {'grade': 'A',
         'sector': '医疗器械',
         'ivr_min': 35,
         'ann_min': 11,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '骨科+神经外科机器人；程序量增长稳健'},
 'TMO': {'grade': 'A',
         'sector': '生命科学',
         'ivr_min': 35,
         'ann_min': 11,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '生命科学仪器+CRO；生物制药客户资本支出联动'},
 'DHR': {'grade': 'A',
         'sector': '生命科学',
         'ivr_min': 35,
         'ann_min': 11,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '分拆Veralto后更聚焦生科；生物加工需求监控'},
 'NKE': {'grade': 'A',
         'sector': '消费品',
         'ivr_min': 40,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '中国市场依赖度高；品牌定价权与库存周转监控'},
 'CMG': {'grade': 'A',
         'sector': '餐饮',
         'ivr_min': 38,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '同店销售增速+数字化订单；食品安全黑天鹅'},
 'LULU': {'grade': 'A',
          'sector': '服装',
          'ivr_min': 50,
          'ann_min': 14,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '高端运动服饰；库存周转率与国际扩张速度'},
 'ULTA': {'grade': 'A',
          'sector': '美妆零售',
          'ivr_min': 45,
          'ann_min': 13,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '美妆渠道垄断；亚马逊竞争与同店销售增速'},
 'SBUX': {'grade': 'A',
          'sector': '消费',
          'ivr_min': 38,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '中国市场依赖高；全球同店销售+APP会员数'},
 'CAT': {'grade': 'A',
         'sector': '工业',
         'ivr_min': 35,
         'ann_min': 11,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '全球基建+矿业周期；中国建设投资联动'},
 'DE': {'grade': 'A',
        'sector': '工业',
        'ivr_min': 35,
        'ann_min': 11,
        'otm_buffer': 0.06,
        'earnings_blackout': 10,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': 'CSP / C→价差',
        'cc_rule': '价差',
        'is_official': False,
        'blackout_desc': '财报前10天',
        'notes': '农机周期；农产品价格与粮食需求联动'},
 'GE': {'grade': 'A',
        'sector': '航空工业',
        'ivr_min': 35,
        'ann_min': 11,
        'otm_buffer': 0.06,
        'earnings_blackout': 10,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': 'CSP / C→价差',
        'cc_rule': '价差',
        'is_official': False,
        'blackout_desc': '财报前10天',
        'notes': 'GE Aerospace纯发动机；订单积压强劲，交付进度'},
 'ETN': {'grade': 'A',
         'sector': '电气化',
         'ivr_min': 35,
         'ann_min': 11,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '电气化+数据中心电力基础设施核心受益者'},
 'COP': {'grade': 'A',
         'sector': '能源',
         'ivr_min': 38,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '油价联动；OPEC+决议前谨慎开仓'},
 'EOG': {'grade': 'A',
         'sector': '能源',
         'ivr_min': 38,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '页岩油低成本龙头；WTI油价为核心指标'},
 'SLB': {'grade': 'A',
         'sector': '油服',
         'ivr_min': 40,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '全球油服龙头；新兴市场油气资本支出联动'},
 'SMH': {'grade': 'A',
         'sector': 'ETF半导体',
         'ivr_min': 38,
         'ann_min': 12,
         'otm_buffer': 0.06,
         'earnings_blackout': 0,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '无（ETF）',
         'notes': 'NVDA+TSM+ASML权重高；注意与个股持仓重叠'},
 'SOXX': {'grade': 'A',
          'sector': 'ETF半导体',
          'ivr_min': 38,
          'ann_min': 12,
          'otm_buffer': 0.06,
          'earnings_blackout': 0,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '无（ETF）',
          'notes': '与SMH高度相关；二选一开仓；出口管制联动'},
 'V': {'grade': 'B',
       'sector': '金融',
       'ivr_min': 28,
       'ann_min': 8,
       'otm_buffer': 0.05,
       'earnings_blackout': 10,
       'fomc_blackout': 0,
       'cpi_blackout': 0,
       'nfp_blackout': 0,
       'boj_blackout': 0,
       'structure': 'CSP（全天候）',
       'cc_rule': '允许×0.5',
       'is_official': True,
       'blackout_desc': '财报前10天',
       'notes': 'MA+V合计≤净值12%；IVR<20%不推也非错误'},
 'GLD': {'grade': 'B',
         'sector': 'ETF商品',
         'ivr_min': 32,
         'ann_min': 8,
         'otm_buffer': 0.05,
         'earnings_blackout': 0,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP',
         'cc_rule': '强制前置',
         'is_official': True,
         'blackout_desc': '无（ETF）',
         'notes': 'VIX>22或地缘触发才推；VIX<18绝对禁止',
         'special_rules': ['gld_vix_gate']},
 'XLU': {'grade': 'B',
         'sector': 'ETF公用事业',
         'ivr_min': 26,
         'ann_min': 7,
         'otm_buffer': 0.05,
         'earnings_blackout': 0,
         'fomc_blackout': 5,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→×0.5',
         'cc_rule': '允许×0.5',
         'is_official': True,
         'blackout_desc': 'FOMC前5天',
         'notes': '利率高度敏感；FOMC窗口最严；C状态仓位×0.5'},
 'SPGI': {'grade': 'B',
          'sector': '金融数据',
          'ivr_min': 28,
          'ann_min': 9,
          'otm_buffer': 0.05,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '信用评级垄断；低波动防御持仓'},
 'CME': {'grade': 'B',
         'sector': '金融交易所',
         'ivr_min': 28,
         'ann_min': 9,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '波动率受益者；VIX高时成交量上升反而利好'},
 'ICE': {'grade': 'B',
         'sector': '金融交易所',
         'ivr_min': 28,
         'ann_min': 9,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '固收市场敞口；利率波动联动；NYSE运营者'},
 'MCO': {'grade': 'B',
         'sector': '金融数据',
         'ivr_min': 30,
         'ann_min': 10,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '信用评级+数据分析双业务；低波防御'},
 'COST': {'grade': 'B',
          'sector': '零售',
          'ivr_min': 30,
          'ann_min': 9,
          'otm_buffer': 0.05,
          'earnings_blackout': 10,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': 'CSP / C→价差',
          'cc_rule': '价差',
          'is_official': False,
          'blackout_desc': '财报前10天',
          'notes': '会员费护城河；低波动防御型；量贩抗通胀属性'},
 'WMT': {'grade': 'B',
         'sector': '零售',
         'ivr_min': 28,
         'ann_min': 8,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '防御零售龙头；广告+金融科技增量业务'},
 'MCD': {'grade': 'B',
         'sector': '餐饮',
         'ivr_min': 28,
         'ann_min': 8,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '特许经营模式；全球收入分散；同店销售核心'},
 'TJX': {'grade': 'B',
         'sector': '折扣零售',
         'ivr_min': 28,
         'ann_min': 9,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '折扣零售；经济下行受益者；库存买手护城河'},
 'HON': {'grade': 'B',
         'sector': '工业',
         'ivr_min': 30,
         'ann_min': 10,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '工业多元化龙头；航空+工业自动化防御属性'},
 'UNP': {'grade': 'B',
         'sector': '工业',
         'ivr_min': 28,
         'ann_min': 9,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '铁路运输垄断；经济先行指标；劳工协议监控'},
 'LMT': {'grade': 'B',
         'sector': '国防',
         'ivr_min': 28,
         'ann_min': 9,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '国防预算刚性；地缘风险时IV升高有利于卖方'},
 'RTX': {'grade': 'B',
         'sector': '国防/航空',
         'ivr_min': 28,
         'ann_min': 9,
         'otm_buffer': 0.05,
         'earnings_blackout': 10,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': 'CSP / C→价差',
         'cc_rule': '价差',
         'is_official': False,
         'blackout_desc': '财报前10天',
         'notes': '发动机+国防双驱动；普惠发动机维修事件监控'},
 'GD': {'grade': 'B',
        'sector': '国防',
        'ivr_min': 28,
        'ann_min': 9,
        'otm_buffer': 0.05,
        'earnings_blackout': 10,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': 'CSP / C→价差',
        'cc_rule': '价差',
        'is_official': False,
        'blackout_desc': '财报前10天',
        'notes': '军舰/陆军系统；国防订单能见度高'},
 'META': {'grade': 'C',
          'sector': '科技',
          'ivr_min': 60,
          'ann_min': 15,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制Bull Put Spread',
          'cc_rule': '暂停',
          'is_official': True,
          'blackout_desc': '财报前14天',
          'notes': '持正股禁卖Put；广告/DAU预报视同财报前14天'},
 'NVDA': {'grade': 'C',
          'sector': '半导体',
          'ivr_min': 65,
          'ann_min': 18,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制Bull Put Spread',
          'cc_rule': '暂停',
          'is_official': True,
          'blackout_desc': '财报前14天',
          'notes': 'BIS出口管制实时监控；NVDA+TSM+ASML≤净值15%'},
 'TSLA': {'grade': 'C',
          'sector': '电动车',
          'ivr_min': 80,
          'ann_min': 20,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制Bull Put Spread',
          'cc_rule': '暂停',
          'is_official': True,
          'blackout_desc': '财报前14天',
          'notes': '🚨最多2张；仅状态A；>200MA；马斯克声明前14天'},
 'HOOD': {'grade': 'C',
          'sector': '金融科技',
          'ivr_min': 65,
          'ann_min': 18,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': True,
          'blackout_desc': '财报前14天',
          'notes': 'BTC单日跌>8%停开；OI≥5000张；加密组合≤4%'},
 'IBIT': {'grade': 'C',
          'sector': 'ETF比特币',
          'ivr_min': 60,
          'ann_min': 15,
          'otm_buffer': 0.05,
          'earnings_blackout': 0,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': True,
          'blackout_desc': '无',
          'notes': 'BTC>50MA；SEC监管动作监控；加密组合≤净值4%'},
 'COIN': {'grade': 'C',
          'sector': '加密交易所',
          'ivr_min': 75,
          'ann_min': 20,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制Bull Put Spread',
          'cc_rule': '暂停',
          'is_official': True,
          'blackout_desc': '财报前14天',
          'notes': '仅状态A；与MSTR互斥持仓；COIN≤净值2%'},
 'MSTR': {'grade': 'C',
          'sector': '加密持仓',
          'ivr_min': 85,
          'ann_min': 25,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制Bull Put Spread',
          'cc_rule': '暂停',
          'is_official': True,
          'blackout_desc': '财报前14天',
          'notes': '🚨最多1张；BTC>$60K且>50MA；与COIN互斥；≤净值1.5%'},
 'CRCL': {'grade': 'C',
          'sector': '稳定币',
          'ivr_min': 80,
          'ann_min': 20,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': True,
          'blackout_desc': '财报前14天',
          'notes': '上市≥180天；OI≥5000；监管立法零容忍；≤净值1%'},
 'NFLX': {'grade': 'C',
          'sector': '流媒体',
          'ivr_min': 55,
          'ann_min': 15,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': '订阅用户+ARPU双指标；广告层级转化率监控'},
 'AMD': {'grade': 'C',
         'sector': '半导体',
         'ivr_min': 60,
         'ann_min': 16,
         'otm_buffer': 0.05,
         'earnings_blackout': 14,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': '强制价差',
         'cc_rule': '暂停',
         'is_official': False,
         'blackout_desc': '财报前14天',
         'notes': 'AI GPU份额vs NVDA竞争；MI系列数据中心季报'},
 'PANW': {'grade': 'C',
          'sector': '网络安全',
          'ivr_min': 55,
          'ann_min': 15,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': '平台化ARR增速核心；Billings指引高度敏感'},
 'CRWD': {'grade': 'C',
          'sector': '网络安全',
          'ivr_min': 65,
          'ann_min': 18,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'ARR增速核心；2024年IT宕机事件后信誉修复进度'},
 'SNOW': {'grade': 'C',
          'sector': '云数据',
          'ivr_min': 70,
          'ann_min': 20,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': '消费型计费（用量驱动）；RPO+NDR增速监控'},
 'DDOG': {'grade': 'C',
          'sector': '云监控',
          'ivr_min': 65,
          'ann_min': 18,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'ARR+NDR双指标；AI Workload监控收益变现节奏'},
 'NET': {'grade': 'C',
         'sector': '网络安全',
         'ivr_min': 65,
         'ann_min': 18,
         'otm_buffer': 0.05,
         'earnings_blackout': 14,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': '强制价差',
         'cc_rule': '暂停',
         'is_official': False,
         'blackout_desc': '财报前14天',
         'notes': 'SASE/SSE架构；大客户RPO增速为核心指标'},
 'SHOP': {'grade': 'C',
          'sector': '电商SaaS',
          'ivr_min': 65,
          'ann_min': 18,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'GMV增速+Take Rate；假日季与Q4财报高敏感'},
 'UBER': {'grade': 'C',
          'sector': '出行平台',
          'ivr_min': 55,
          'ann_min': 15,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'Trips增速+EBITDA利润率；自动驾驶竞争压力'},
 'ABNB': {'grade': 'C',
          'sector': '旅行平台',
          'ivr_min': 55,
          'ann_min': 15,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'GN增速+ADR；旅游季节性（Q2/Q3峰值窗口）'},
 'MU': {'grade': 'C',
        'sector': '半导体',
        'ivr_min': 55,
        'ann_min': 15,
        'otm_buffer': 0.05,
        'earnings_blackout': 14,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': '强制价差',
        'cc_rule': '暂停',
        'is_official': False,
        'blackout_desc': '财报前14天',
        'notes': 'DRAM/HBM内存周期；AI服务器HBM需求联动'},
 'MRVL': {'grade': 'C',
          'sector': '半导体',
          'ivr_min': 55,
          'ann_min': 15,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': '定制AI芯片（ASIC）龙头；云客户集中度风险'},
 'ON': {'grade': 'C',
        'sector': '半导体',
        'ivr_min': 55,
        'ann_min': 15,
        'otm_buffer': 0.05,
        'earnings_blackout': 14,
        'fomc_blackout': 0,
        'cpi_blackout': 0,
        'nfp_blackout': 0,
        'boj_blackout': 0,
        'structure': '强制价差',
        'cc_rule': '暂停',
        'is_official': False,
        'blackout_desc': '财报前14天',
        'notes': '汽车+工业模拟芯片；EV渗透率下行压力监控'},
 'ARM': {'grade': 'C',
         'sector': '半导体IP',
         'ivr_min': 70,
         'ann_min': 20,
         'otm_buffer': 0.05,
         'earnings_blackout': 14,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': '强制价差',
         'cc_rule': '暂停',
         'is_official': False,
         'blackout_desc': '财报前14天',
         'notes': 'ATA授权模式；软银持股集中；AI边缘授权进展'},
 'PYPL': {'grade': 'C',
          'sector': '金融科技',
          'ivr_min': 55,
          'ann_min': 15,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'TPV增速+Take Rate；苹果Pay/Google Pay竞争'},
 'SMCI': {'grade': 'C',
          'sector': '服务器',
          'ivr_min': 80,
          'ann_min': 22,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': '🚨最多2张；会计违规历史，审计风险极高，流动性必验'},
 'PLTR': {'grade': 'C',
          'sector': '数据分析',
          'ivr_min': 70,
          'ann_min': 20,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': '政府+商业双线；AI Platform商业化速度核心指标'},
 'RBLX': {'grade': 'C',
          'sector': '游戏/元宇宙',
          'ivr_min': 65,
          'ann_min': 18,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'DAU+消费时长；青少年用户监管与货币化风险'},
 'DKNG': {'grade': 'C',
          'sector': '体育博彩',
          'ivr_min': 65,
          'ann_min': 18,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'Hold Rate季度波动大；各州执照合规进展监控'},
 'DASH': {'grade': 'C',
          'sector': '外卖平台',
          'ivr_min': 65,
          'ann_min': 18,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'GOV增速+EBITDA；劳工分类（员工vs承包商）立法风险'},
 'RDDT': {'grade': 'C',
          'sector': '社交媒体',
          'ivr_min': 70,
          'ann_min': 20,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': 'IPO较新，历史IV参考有限；AI数据授权收入变现'},
 'BABA': {'grade': 'C',
          'sector': '中概/电商',
          'ivr_min': 65,
          'ann_min': 18,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': '中概监管/ADR退市风险持续；云分拆进展监控'},
 'PDD': {'grade': 'C',
         'sector': '中概/电商',
         'ivr_min': 70,
         'ann_min': 20,
         'otm_buffer': 0.05,
         'earnings_blackout': 14,
         'fomc_blackout': 0,
         'cpi_blackout': 0,
         'nfp_blackout': 0,
         'boj_blackout': 0,
         'structure': '强制价差',
         'cc_rule': '暂停',
         'is_official': False,
         'blackout_desc': '财报前14天',
         'notes': 'Temu全球化；美国关税+中概监管双重尾部风险'},
 'MARA': {'grade': 'C',
          'sector': '比特币矿业',
          'ivr_min': 85,
          'ann_min': 25,
          'otm_buffer': 0.05,
          'earnings_blackout': 14,
          'fomc_blackout': 0,
          'cpi_blackout': 0,
          'nfp_blackout': 0,
          'boj_blackout': 0,
          'structure': '强制价差',
          'cc_rule': '暂停',
          'is_official': False,
          'blackout_desc': '财报前14天',
          'notes': '🚨最多1张；BTC价格1:1联动；哈希率+算力电费成本'}}

OFFICIAL_SYMBOLS = frozenset(_DEFAULT_TICKERS.keys())


_REQUIRED_TICKER_DEFAULTS = {
    "grade":             "C",
    "ivr_min":           50,
    "ann_min":           10,
    "otm_buffer":        0.05,
    "earnings_blackout": 0,
    "structure":         "CSP",
}


def _normalize_ticker_row(sym: str, cfg: dict) -> dict:
    """合并内置默认与外部 JSON；补全元数据字段。

    兼容：`is_official` 与 `official` 同义。
    """
    base = _DEFAULT_TICKERS.get(sym, {})
    merged = {**base, **cfg}

    # official / is_official 双写兼容
    if "official" not in merged and "is_official" in merged:
        merged["official"] = bool(merged.get("is_official"))
    if "official" not in merged:
        merged["official"] = sym in OFFICIAL_SYMBOLS

    merged.setdefault("sector",  base.get("sector",  "Other"))
    merged.setdefault("cc_rule", base.get("cc_rule", ""))
    for k in ("cpi_blackout", "nfp_blackout", "boj_blackout", "fomc_blackout"):
        if k not in merged:
            merged[k] = base.get(k, 0)
    # 必填字段兜底，避免外部 JSON 缺字段在 process_ticker 中 KeyError
    for k, default_v in _REQUIRED_TICKER_DEFAULTS.items():
        if k not in merged or merged.get(k) is None:
            merged[k] = base.get(k, default_v)
    return merged


def normalize_tickers(raw: dict) -> dict:
    return {s: _normalize_ticker_row(s, c) for s, c in raw.items()}


TICKERS = normalize_tickers(dict(_DEFAULT_TICKERS))



# ═══════════════════════════════════════════════════════════
#  重试装饰器（区分可重试与不可重试错误）
# ═══════════════════════════════════════════════════════════

class _NonRetryableError(Exception):
    """认证/权限错误（401/403），不应重试"""
    pass

def retry(max_attempts=3, delay=2, backoff=1.5):
    """
    指数退避重试装饰器。
    HTTP 429（限流）→ 可重试。
    HTTP 401/403（认证失败）→ 直接抛出，不重试（避免锁号）。
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            wait = delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except _NonRetryableError:
                    raise  # 认证错误：直接上抛，不重试
                except requests.exceptions.HTTPError as e:
                    status = e.response.status_code if e.response is not None else 0
                    if status in (401, 403):
                        raise _NonRetryableError(f"认证失败({status})，停止重试：{e}") from e
                    last_exc = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"{func.__name__} 第{attempt+1}次失败（{wait:.1f}s后重试）：{e}")
                        time.sleep(wait)
                        wait *= backoff
                except Exception as e:
                    last_exc = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"{func.__name__} 第{attempt+1}次失败（{wait:.1f}s后重试）：{e}")
                        time.sleep(wait)
                        wait *= backoff
            logger.error(f"{func.__name__} 全部 {max_attempts} 次均失败：{last_exc}")
            raise last_exc
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════
#  MassiveKeyPool（失效密钥追踪 + 等待时间竞态修复）
# ═══════════════════════════════════════════════════════════

class MassiveKeyPool:
    """
    线程安全的多密钥轮转池。

    - mark_invalid()：401/403 时标记密钥失效并排除
    - 等待时间在锁内计算，避免多线程竞态
    - 全部密钥失效时抛出明确异常
    """

    def __init__(self, keys: list, min_gap: float = 20.0):
        if not keys:
            raise ValueError("密钥列表不能为空，请在 .env 中配置 MASSIVE_KEYS")
        self._slots: list[tuple[str, float]] = [(k, 0.0) for k in keys]
        self._lock    = threading.Lock()
        self._min_gap = min_gap
        self._invalid: set[str] = set()

    @property
    def key_count(self) -> int:
        return len(self._slots)

    @property
    def valid_key_count(self) -> int:
        with self._lock:
            return sum(1 for k, _ in self._slots if k not in self._invalid)

    @property
    def min_gap(self) -> float:
        return self._min_gap

    def mark_invalid(self, key: str):
        """将密钥标记为永久失效（401/403时调用）"""
        with self._lock:
            self._invalid.add(key)
        logger.warning(f"Massive密钥 ...{key[-6:]} 已标记为失效（401/403），从轮转中排除")

    def acquire(self, timeout: float = 120.0) -> str:
        """获取可用密钥（阻塞直到有密钥冷却完毕）"""
        deadline = time.monotonic() + timeout
        while True:
            now = time.monotonic()
            if now > deadline:
                raise TimeoutError(
                    f"Massive KeyPool 等待超时（>{timeout:.0f}s）——"
                    f"所有密钥均处于冷却期或已失效"
                )
            with self._lock:
                # 过滤有效密钥（排除已标记失效的）
                valid = [(i, k, t) for i, (k, t) in enumerate(self._slots)
                         if k not in self._invalid]
                if not valid:
                    raise RuntimeError(
                        "所有 Massive 密钥均已失效（401/403），请更新 .env 中的 MASSIVE_KEYS"
                    )

                # 找出已冷却的密钥
                ready = [(i, k, t) for i, k, t in valid if (now - t) >= self._min_gap]
                if ready:
                    idx, key, _ = max(ready, key=lambda x: now - x[2])
                    self._slots[idx] = (key, now)
                    return key

                # 等待时间在锁内计算（避免竞态）
                wait_secs = min(self._min_gap - (now - t) for _, _, t in valid)

            # 锁外等待
            time.sleep(min(wait_secs + 0.05, 1.0))

    def status(self) -> list[dict]:
        now = time.monotonic()
        with self._lock:
            return [
                {
                    "key_suffix":        k[-6:],
                    "cooldown_remaining": max(0.0, round(self._min_gap - (now - t), 1)),
                    "ready":             (now - t) >= self._min_gap,
                    "invalid":           k in self._invalid,
                }
                for k, t in self._slots
            ]


# ═══════════════════════════════════════════════════════════
#  数据源抽象接口
# ═══════════════════════════════════════════════════════════

class DataSource(ABC):
    name: str = "base"

    @abstractmethod
    def get_price(self, ticker: str) -> Optional[float]: ...

    @abstractmethod
    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        """返回 Puts DataFrame：strike/bid/ask/lastPrice/volume/openInterest/impliedVolatility/delta/theta/has_real_greeks"""
        ...

    @abstractmethod
    def get_history(self, ticker: str, period: str) -> pd.DataFrame:
        """返回历史日线 DataFrame（含 Close/High/Low/Volume 列）"""
        ...

    @abstractmethod
    def get_option_dates(self, ticker: str) -> list: ...

    def is_available(self) -> bool:
        return True


# ─── T0 Massive API ─────────────────────────────────────

class MassiveSource(DataSource):
    """T0 主力数据源：Massive API；401/403 时 mark_invalid() 排除密钥。"""
    name = "massive"

    def __init__(self, key_pool: MassiveKeyPool, base_url: str, endpoints: dict):
        self._pool      = key_pool
        self._base      = base_url.rstrip("/")
        self._endpoints = endpoints
        self._session   = requests.Session()
        self._session.headers.update({
            "Accept":     "application/json",
            "User-Agent": "OpenClaw/3.0",
        })

    def is_available(self) -> bool:
        return self._pool.valid_key_count > 0 and bool(self._base)

    def _get(self, endpoint_key: str, params: dict = None, timeout: int = 20) -> dict:
        key  = self._pool.acquire()
        path = self._endpoints.get(endpoint_key, endpoint_key)
        url  = f"{self._base}/{path}"
        p    = dict(params or {})
        p["apikey"] = key
        try:
            r = self._session.get(url, params=p, timeout=timeout)
            if r.status_code == 401 or r.status_code == 403:
                self._pool.mark_invalid(key)
                raise _NonRetryableError(f"Massive 密钥认证失败({r.status_code})")
            if r.status_code == 429:
                raise RuntimeError(f"Massive API 速率超限（429），密钥末位…{key[-6:]}")
            r.raise_for_status()
            return r.json()
        except _NonRetryableError:
            raise
        except Exception:
            raise

    def _parse_price(self, data: dict) -> Optional[float]:
        for field in ("last", "price", "close", "mark"):
            v = data.get(field)
            if v is not None:
                return float(v)
        d = data.get("data") or data.get("quote") or {}
        if isinstance(d, dict):
            for field in ("last", "price", "close", "mark"):
                v = d.get(field)
                if v is not None:
                    return float(v)
        results = data.get("results") or data.get("quotes") or []
        if isinstance(results, list) and results:
            item = results[0]
            for field in ("c", "last", "price", "close"):
                v = item.get(field)
                if v is not None:
                    return float(v)
        return None

    def _parse_expirations(self, data: dict) -> list:
        for field in ("expirations", "expiration_dates", "dates", "expiryDates"):
            v = data.get(field)
            if isinstance(v, list):
                return [str(x) for x in v if x]
        results = data.get("results") or data.get("data") or []
        if isinstance(results, list):
            dates = sorted(set(
                r.get("expiration_date") or r.get("expiry") or r.get("date") or ""
                for r in results if isinstance(r, dict)
            ))
            return [d for d in dates if d]
        return []

    def _parse_chain(self, data: dict) -> list[dict]:
        rows = None
        for key in ("options", "results", "data", "contracts"):
            v = data.get(key)
            if isinstance(v, list):
                rows = v
                break
            if isinstance(v, dict):
                puts = v.get("puts") or v.get("put") or []
                if isinstance(puts, list):
                    rows = puts
                    break

        if not rows:
            return []

        def _f(d, *keys, default=0.0):
            for k in keys:
                v = d.get(k)
                if v is not None:
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        pass
            return default

        def _i(d, *keys, default=0):
            for k in keys:
                v = d.get(k)
                if v is not None:
                    try:
                        return int(float(v))
                    except (TypeError, ValueError):
                        pass
            return default

        out = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            opt_type = (r.get("type") or r.get("option_type") or r.get("contract_type") or "").lower()
            if opt_type and opt_type not in ("put", "p"):
                continue

            greeks  = r.get("greeks") or r.get("greek") or {}
            details = r.get("details") or r.get("contract") or {}
            last_q  = r.get("last_quote") or r.get("quote") or {}
            day_dat = r.get("day") or {}

            strike = _f(r, "strike", "strike_price") or _f(details, "strike_price", "strike")
            bid    = _f(r, "bid") or _f(last_q, "bid")
            ask    = _f(r, "ask") or _f(last_q, "ask")
            last   = _f(r, "last", "lastPrice", "close") or _f(day_dat, "close")
            vol    = _i(r, "volume") or _i(day_dat, "volume")
            oi     = _i(r, "open_interest", "openInterest")
            iv     = _f(r, "implied_volatility", "iv", "impliedVolatility") or _f(greeks, "mid_iv", "iv")
            delta  = _f(r, "delta") or _f(greeks, "delta")
            theta  = _f(r, "theta") or _f(greeks, "theta")
            has_real = bool(greeks) or (delta != 0.0)

            out.append({
                "strike":            strike,
                "bid":               bid,
                "ask":               ask,
                "lastPrice":         last,
                "volume":            vol,
                "openInterest":      oi,
                "impliedVolatility": iv,
                "delta":             delta,
                "theta":             theta,
                "has_real_greeks":   has_real,
            })
        return out

    def _parse_history(self, data: dict) -> pd.DataFrame:
        rows = None
        for field in ("bars", "results", "data", "candles", "ohlcv"):
            v = data.get(field)
            if isinstance(v, list) and v:
                rows = v
                break
        if rows is None:
            hist = data.get("history") or {}
            if isinstance(hist, dict):
                rows = hist.get("day") or []
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        rename_map = {
            "c": "Close", "h": "High", "l": "Low", "v": "Volume",
            "close": "Close", "high": "High", "low": "Low", "volume": "Volume",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        for col in ("Close", "High", "Low", "Volume"):
            if col not in df.columns:
                df[col] = np.nan
            df[col] = pd.to_numeric(df[col], errors="coerce")
        for ts_col in ("date", "t", "timestamp", "datetime", "time"):
            if ts_col in df.columns:
                try:
                    if ts_col == "t":
                        df.index = pd.to_datetime(df[ts_col], unit="ms")
                    else:
                        df.index = pd.to_datetime(df[ts_col])
                except Exception:
                    pass
                break
        return df

    def get_price(self, ticker: str) -> Optional[float]:
        data = self._get("quote", {"symbol": ticker})
        return self._parse_price(data)

    def get_option_dates(self, ticker: str) -> list:
        data = self._get("expirations", {"symbol": ticker, "contract_type": "put"})
        return self._parse_expirations(data)

    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        data = self._get("chain", {
            "symbol": ticker, "expiration": expiry,
            "expiration_date": expiry, "type": "put", "contract_type": "put",
        })
        rows = self._parse_chain(data)
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def get_history(self, ticker: str, period: str) -> pd.DataFrame:
        days_map = {"1d":1,"5d":5,"1mo":30,"3mo":90,"6mo":180,"1y":365,"2y":730}
        days  = days_map.get(period, 365)
        start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        end   = datetime.now().strftime("%Y-%m-%d")
        data  = self._get("history", {
            "symbol": ticker, "from": start, "start": start,
            "to": end, "end": end, "interval": "day", "timespan": "day", "adjusted": "true",
        })
        return self._parse_history(data)


# ─── Alpaca Paper（OPTIONS_ROUTER 主数据源）─────────────

class AlpacaPaperSource(DataSource):
    """
    OPTIONS_ROUTER 备用数据源：Alpaca Paper API + Market Data Snapshots
    校验 bid/ask 有效性，过滤零价合约
    """
    name = "alpaca"

    def __init__(self, paper_key: str, paper_secret: str,
                 paper_base_url: str = "https://paper-api.alpaca.markets/v2",
                 data_base_url: str = "https://data.alpaca.markets"):
        self._paper_key    = paper_key
        self._paper_secret = paper_secret
        self._paper_base   = paper_base_url.rstrip("/")
        self._data_base    = data_base_url.rstrip("/")

        auth_headers = {
            "APCA-API-KEY-ID":     self._paper_key,
            "APCA-API-SECRET-KEY": self._paper_secret,
        }
        common = {"Accept": "application/json", "User-Agent": "OpenClaw/3.0"}
        self._paper_session = requests.Session()
        self._data_session  = requests.Session()
        self._paper_session.headers.update({**common, **auth_headers})
        self._data_session.headers.update({**common, **auth_headers})

    def is_available(self) -> bool:
        return bool(self._paper_key and self._paper_secret)

    def _paper_get(self, path: str, params: dict = None, timeout: int = 20) -> dict:
        url = f"{self._paper_base}{path}"
        r   = self._paper_session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def _data_get(self, path: str, params: dict = None, timeout: int = 20) -> dict:
        url = f"{self._data_base}{path}"
        r   = self._data_session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _chunk(items: list, n: int):
        for i in range(0, len(items), n):
            yield items[i:i+n]

    def get_price(self, ticker: str) -> Optional[float]:
        return None

    def get_history(self, ticker: str, period: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_option_dates(self, ticker: str) -> list:
        today      = datetime.now().date()
        end_days   = max(DTE_PREFERRED_MAX, DTE_FALLBACK_MAX) + 20
        start_date = (today - timedelta(days=5)).strftime("%Y-%m-%d")
        end_date   = (today + timedelta(days=end_days)).strftime("%Y-%m-%d")

        dates      = set()
        page_token = None
        while True:
            params = {
                "underlying_symbols": ticker, "type": "put", "status": "active",
                "expiration_date_gte": start_date, "expiration_date_lte": end_date,
                "limit": 10000,
            }
            if page_token:
                params["page_token"] = page_token
            data = self._paper_get("/options/contracts", params=params)
            for c in data.get("option_contracts", []) or []:
                ed = c.get("expiration_date")
                if ed:
                    dates.add(str(ed)[:10])
            page_token = data.get("next_page_token")
            if not page_token:
                break
        return sorted(dates)

    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        # Step 1: 拉合约列表（含 OI 和 strike）
        contracts_by_symbol: dict[str, dict] = {}
        symbols: list[str] = []
        page_token = None
        while True:
            params = {
                "underlying_symbols": ticker, "type": "put",
                "status": "active", "expiration_date": expiry, "limit": 10000,
            }
            if page_token:
                params["page_token"] = page_token
            data = self._paper_get("/options/contracts", params=params)
            for c in data.get("option_contracts", []) or []:
                sym = c.get("symbol")
                if not sym:
                    continue
                try:
                    strike = float(c.get("strike_price", 0))
                except (TypeError, ValueError):
                    strike = 0.0
                try:
                    oi = int(float(c.get("open_interest", 0) or 0))
                except (TypeError, ValueError):
                    oi = 0
                try:
                    last_price = float(c.get("close_price", 0))
                except (TypeError, ValueError):
                    last_price = 0.0

                if sym not in contracts_by_symbol:
                    contracts_by_symbol[sym] = {
                        "strike": strike, "openInterest": oi, "lastPrice": last_price,
                    }
                    symbols.append(sym)
            page_token = data.get("next_page_token")
            if not page_token:
                break

        if not symbols:
            return pd.DataFrame()

        # Step 2: 批量拉快照（bid/ask/Greeks）
        rows = []
        for batch in self._chunk(symbols, 100):
            params    = {"symbols": ",".join(batch)}
            snap_resp = self._data_get("/v1beta1/options/snapshots", params=params)
            snapshots = snap_resp.get("snapshots", {}) or {}

            for sym in batch:
                snap = snapshots.get(sym)
                base = contracts_by_symbol.get(sym)
                if not snap or not base:
                    continue

                qt     = snap.get("latestQuote") or {}
                greeks = snap.get("greeks") or {}

                try:
                    bid = float(qt.get("bp", 0) or 0)
                except (TypeError, ValueError):
                    bid = 0.0
                try:
                    ask = float(qt.get("ap", 0) or 0)
                except (TypeError, ValueError):
                    ask = 0.0

                # 零价合约过滤（Paper 等源常见）
                if bid <= 0 or ask <= 0:
                    logger.debug(f"[alpaca] {sym} bid/ask为零，跳过（Paper数据问题）")
                    continue

                try:
                    iv = float(snap.get("impliedVolatility", 0) or 0)
                except (TypeError, ValueError):
                    iv = 0.0

                try:
                    theta = float(greeks.get("theta", 0) or 0)
                except (TypeError, ValueError):
                    theta = 0.0
                try:
                    delta = float(greeks.get("delta", 0) or 0)
                except (TypeError, ValueError):
                    delta = 0.0

                daily_bar  = snap.get("dailyBar") or {}
                minute_bar = snap.get("minuteBar") or {}
                vol = 0
                for bar in (daily_bar, minute_bar):
                    for k in ("v", "volume"):
                        v = bar.get(k)
                        if v is not None:
                            try:
                                vol = int(float(v))
                                break
                            except (TypeError, ValueError):
                                pass
                    if vol:
                        break

                latest_trade = snap.get("latestTrade") or {}
                try:
                    last_trade = float(latest_trade.get("p", 0) or 0)
                except (TypeError, ValueError):
                    last_trade = 0.0
                last_price = last_trade if last_trade > 0 else float(base.get("lastPrice", 0) or 0)

                rows.append({
                    "strike":            base.get("strike", 0.0),
                    "bid":               bid,
                    "ask":               ask,
                    "lastPrice":         last_price,
                    "volume":            vol,
                    "openInterest":      int(base.get("openInterest", 0) or 0),
                    "impliedVolatility": iv,
                    "delta":             delta,
                    "theta":             theta,
                    "has_real_greeks":   bool(greeks),
                })

        cols = ["strike","bid","ask","lastPrice","volume","openInterest",
                "impliedVolatility","delta","theta","has_real_greeks"]
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(rows, columns=cols)


# ─── CBOE Delayed Quotes（公开端点，无需密钥）────────────

class CboeSource(DataSource):
    """
    CBOE 延迟行情公开端点：
    https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json

    - 无需注册；要求基础 UA/Referer 请求头
    - 只提供快照，不提供历史 K 线
    """
    name = "cboe"

    def __init__(self, request_gap_sec: float = 1.0):
        self._base = "https://cdn.cboe.com/api/global/delayed_quotes/options"
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.cboe.com/",
            "Accept": "application/json",
        })
        self._cache: dict[str, tuple[float, dict]] = {}
        self._lock = threading.Lock()
        self._cache_ttl_sec = 20.0
        self._last_request_ts = 0.0
        self._request_gap_sec = max(0.0, float(request_gap_sec))

    def is_available(self) -> bool:
        return True

    @staticmethod
    def _to_cboe_symbol(ticker: str) -> str:
        """
        CBOE 指数路径兼容：
        - ^VIX -> _VIX
        - ^SPX -> _SPX
        """
        t = str(ticker or "").strip()
        if t.startswith("^"):
            return "_" + t[1:]
        return t

    def _fetch_snapshot(self, ticker: str) -> dict:
        now = time.time()
        normalized = self._to_cboe_symbol(ticker)
        with self._lock:
            hit = self._cache.get(normalized)
            if hit and (now - hit[0]) <= self._cache_ttl_sec:
                return hit[1]

            # 限速：避免高并发触发 CBOE 临时封禁
            elapsed = now - self._last_request_ts
            if elapsed < self._request_gap_sec:
                time.sleep(self._request_gap_sec - elapsed)
            self._last_request_ts = time.time()

        url = f"{self._base}/{normalized}.json"
        r = self._session.get(url, timeout=12)
        r.raise_for_status()
        data = r.json()

        with self._lock:
            self._cache[normalized] = (time.time(), data)
        return data

    @staticmethod
    def _parse_occ_option_symbol(sym: str):
        """
        OCC 格式：SPY260516P00650000
        返回 (ticker, expiry_yyyy_mm_dd, option_type, strike)
        """
        m = re.match(r"^([A-Z]+)(\d{6})([CP])(\d{8})$", str(sym or ""))
        if not m:
            return None
        ticker = m.group(1)
        expiry = datetime.strptime(m.group(2), "%y%m%d").strftime("%Y-%m-%d")
        opt_type = "put" if m.group(3) == "P" else "call"
        strike = int(m.group(4)) / 1000.0
        return ticker, expiry, opt_type, strike

    def get_price(self, ticker: str) -> Optional[float]:
        data = self._fetch_snapshot(ticker)
        d = data.get("data", {}) if isinstance(data, dict) else {}
        px = d.get("current_price") or d.get("close")
        return float(px) if px is not None else None

    def get_history(self, ticker: str, period: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_option_dates(self, ticker: str) -> list:
        data = self._fetch_snapshot(ticker)
        rows = (data.get("data", {}) or {}).get("options", []) if isinstance(data, dict) else []
        dates = set()
        for o in rows or []:
            occ = o.get("option") or o.get("option_symbol") or o.get("symbol")
            parsed = self._parse_occ_option_symbol(occ)
            if not parsed:
                continue
            _, expiry, opt_type, _ = parsed
            if opt_type == "put":
                dates.add(expiry)
        return sorted(dates)

    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        data = self._fetch_snapshot(ticker)
        rows = (data.get("data", {}) or {}).get("options", []) if isinstance(data, dict) else []
        out = []

        for o in rows or []:
            occ = o.get("option") or o.get("option_symbol") or o.get("symbol")
            parsed = self._parse_occ_option_symbol(occ)
            if not parsed:
                continue
            _, exp, opt_type, strike = parsed
            if opt_type != "put" or exp != expiry:
                continue

            bid = float(o.get("bid", 0) or 0)
            ask = float(o.get("ask", 0) or 0)
            last = float(o.get("last_trade_price", 0) or o.get("last", 0) or 0)
            iv = float(o.get("iv", 0) or o.get("implied_volatility", 0) or 0)
            delta = float(o.get("delta", 0) or 0)
            theta = float(o.get("theta", 0) or 0)
            oi = int(float(o.get("open_interest", 0) or 0))
            vol = int(float(o.get("volume", 0) or 0))

            out.append({
                "strike":            strike,
                "bid":               bid,
                "ask":               ask,
                "lastPrice":         last,
                "volume":            vol,
                "openInterest":      oi,
                "impliedVolatility": iv,
                "delta":             delta,
                "theta":             theta,
                "has_real_greeks":   bool(delta or theta),
            })

        cols = ["strike", "bid", "ask", "lastPrice", "volume", "openInterest",
                "impliedVolatility", "delta", "theta", "has_real_greeks"]
        if not out:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(out, columns=cols)


# ─── T1 Tradier ──────────────────────────────────────────

class TradierSource(DataSource):
    """T1 备用数据源：Tradier API（真实Greeks + IV）"""
    name = "tradier"

    def __init__(self, token: str, sandbox: bool = False):
        self.token   = token
        self.base    = ("https://sandbox.tradier.com/v1"
                        if sandbox else "https://api.tradier.com/v1")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept":        "application/json",
        })

    def is_available(self) -> bool:
        return bool(self.token)

    def _get(self, path: str, params: dict = None, timeout: int = 10) -> dict:
        r = self.session.get(f"{self.base}{path}", params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def get_price(self, ticker: str) -> Optional[float]:
        data  = self._get("/markets/quotes", {"symbols": ticker})
        quote = data.get("quotes", {}).get("quote", {})
        v     = quote.get("last") or quote.get("close")
        return float(v) if v else None

    def get_option_dates(self, ticker: str) -> list:
        data  = self._get("/markets/options/expirations",
                          {"symbol": ticker, "includeAllRoots": "true"})
        dates = data.get("expirations", {}).get("date", [])
        return [dates] if isinstance(dates, str) else (dates or [])

    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        data    = self._get("/markets/options/chains",
                            {"symbol": ticker, "expiration": expiry, "greeks": "true"})
        options = data.get("options", {})
        if not options:
            return pd.DataFrame()
        raw  = options.get("option", [])
        if isinstance(raw, dict):
            raw = [raw]
        puts = [o for o in (raw or []) if o.get("option_type") == "put"]
        if not puts:
            return pd.DataFrame()
        rows = []
        for o in puts:
            g = o.get("greeks") or {}
            rows.append({
                "strike":            float(o.get("strike", 0)),
                "bid":               float(o.get("bid",    0) or 0),
                "ask":               float(o.get("ask",    0) or 0),
                "lastPrice":         float(o.get("last",   0) or 0),
                "volume":            int(o.get("volume",         0) or 0),
                "openInterest":      int(o.get("open_interest",  0) or 0),
                "impliedVolatility": float(g.get("mid_iv", 0) or 0),
                "delta":             float(g.get("delta",  0) or 0),
                "theta":             float(g.get("theta",  0) or 0),
                "has_real_greeks":   bool(g),
            })
        return pd.DataFrame(rows)

    def get_history(self, ticker: str, period: str) -> pd.DataFrame:
        days_map = {"1d":1,"5d":5,"1mo":30,"3mo":90,"6mo":180,"1y":365,"2y":730}
        days  = days_map.get(period, 365)
        start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        end   = datetime.now().strftime("%Y-%m-%d")
        data  = self._get("/markets/history",
                          {"symbol": ticker, "interval": "daily", "start": start, "end": end})
        history = data.get("history") or {}
        raw = history.get("day", [])
        if not raw:
            return pd.DataFrame()
        if isinstance(raw, dict):
            raw = [raw]
        df = pd.DataFrame(raw)
        for col, src in [("Close","close"),("High","high"),("Low","low"),("Volume","volume")]:
            df[col] = pd.to_numeric(df.get(src, 0), errors="coerce")
        df.index = pd.to_datetime(df["date"])
        return df


# ─── T2 Polygon ──────────────────────────────────────────

class PolygonSource(DataSource):
    """T2 备用数据源：Polygon.io（5 req/min 限速）"""
    name = "polygon"
    BASE = "https://api.polygon.io"

    def __init__(self, api_key: str):
        self.api_key    = api_key
        self.session    = requests.Session()
        self._last_call = 0.0
        self._min_gap   = 12.5
        self._lock      = threading.Lock()

    def is_available(self) -> bool:
        return bool(self.api_key)

    def _get(self, path: str, params: dict = None, timeout: int = 15) -> dict:
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self._min_gap:
                time.sleep(self._min_gap - elapsed)
            self._last_call = time.time()
        params = dict(params or {})
        params["apiKey"] = self.api_key
        r = self.session.get(f"{self.BASE}{path}", params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def get_price(self, ticker: str) -> Optional[float]:
        data = self._get(f"/v2/last/trade/{ticker}")
        p    = data.get("results", {}).get("p")
        return float(p) if p else None

    def get_option_dates(self, ticker: str) -> list:
        data    = self._get(f"/v3/snapshot/options/{ticker}",
                            {"limit": 250, "contract_type": "put"})
        results = data.get("results", [])
        return sorted(set(
            r.get("details", {}).get("expiration_date", "")
            for r in results if r.get("details", {}).get("expiration_date")
        ))

    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        data    = self._get(f"/v3/snapshot/options/{ticker}",
                            {"expiration_date": expiry, "contract_type": "put", "limit": 250})
        results = data.get("results", [])
        if not results:
            return pd.DataFrame()
        rows = []
        for r in results:
            det = r.get("details", {})
            day = r.get("day", {})
            g   = r.get("greeks", {})
            lq  = r.get("last_quote", {})
            rows.append({
                "strike":            float(det.get("strike_price", 0)),
                "bid":               float(lq.get("bid", 0) or 0),
                "ask":               float(lq.get("ask", 0) or 0),
                "lastPrice":         float(day.get("close", 0) or 0),
                "volume":            int(day.get("volume", 0) or 0),
                "openInterest":      int(r.get("open_interest", 0) or 0),
                "impliedVolatility": float(r.get("implied_volatility", 0) or 0),
                "delta":             float(g.get("delta", 0) or 0),
                "theta":             float(g.get("theta", 0) or 0),
                "has_real_greeks":   bool(g),
            })
        return pd.DataFrame(rows)

    def get_history(self, ticker: str, period: str) -> pd.DataFrame:
        days_map = {"1d":1,"5d":5,"1mo":30,"3mo":90,"6mo":180,"1y":365,"2y":730}
        days  = days_map.get(period, 365)
        start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        end   = datetime.now().strftime("%Y-%m-%d")
        data  = self._get(f"/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}",
                          {"adjusted": "true", "sort": "asc", "limit": 500})
        results = data.get("results", [])
        if not results:
            return pd.DataFrame()
        df = pd.DataFrame(results)
        df["Close"]  = pd.to_numeric(df.get("c", pd.Series(dtype=float)), errors="coerce")
        df["High"]   = pd.to_numeric(df.get("h", pd.Series(dtype=float)), errors="coerce")
        df["Low"]    = pd.to_numeric(df.get("l", pd.Series(dtype=float)), errors="coerce")
        df["Volume"] = pd.to_numeric(df.get("v", pd.Series(dtype=float)), errors="coerce")
        df.index = pd.to_datetime(df["t"], unit="ms")
        return df


# ─── T3 yfinance（兜底）─────────────────────────────────

class YFinanceSource(DataSource):
    """T3 兜底数据源：yfinance（非官方）。Greeks 需 BS 估算。"""
    name = "yfinance"

    def is_available(self) -> bool:
        return _YF_AVAILABLE

    def get_price(self, ticker: str) -> Optional[float]:
        hist = yf.Ticker(ticker).history(period="5d")
        return float(hist["Close"].iloc[-1]) if not hist.empty else None

    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        puts = yf.Ticker(ticker).option_chain(expiry).puts.copy()
        puts["has_real_greeks"] = False
        if "delta" not in puts.columns:
            puts["delta"] = 0.0
        if "theta" not in puts.columns:
            puts["theta"] = 0.0
        return puts

    def get_history(self, ticker: str, period: str) -> pd.DataFrame:
        return yf.Ticker(ticker).history(period=period)

    def get_option_dates(self, ticker: str) -> list:
        return list(yf.Ticker(ticker).options)


# ─── DataRouter（多源自动路由）───────────────────────────

class DataRouter:
    def __init__(self, sources: list, preferred: str = "auto"):
        self.sources   = sources
        self.health    = {s.name: 100 for s in sources}
        self.preferred = preferred
        self.priority  = self._build_priority(sources, preferred)

    def _build_priority(self, sources, preferred):
        if preferred != "auto":
            forced = [s for s in sources if s.name == preferred]
            if not forced:
                logger.warning(f"指定数据源 {preferred} 不存在，退回 auto")
            else:
                return forced + [s for s in sources if s.name != preferred]
        return [s for s in sources if s.is_available()] or sources

    def _best(self) -> DataSource:
        avail = [s for s in self.priority if s.is_available()]
        if not avail:
            raise RuntimeError("无可用数据源，请检查 API Key 配置或安装 yfinance")
        return max(avail, key=lambda s: self.health[s.name])

    def call(self, method: str, *args, **kwargs):
        last_err = None
        for source in self.priority:
            if not source.is_available():
                continue
            try:
                result = getattr(source, method)(*args, **kwargs)
                # 期权链/到期日：若当前源返回空结果，继续回退到下一个源
                if method in ("get_option_chain", "get_option_dates"):
                    is_empty_df = isinstance(result, pd.DataFrame) and result.empty
                    is_empty_list = isinstance(result, list) and len(result) == 0
                    if is_empty_df or is_empty_list:
                        raise RuntimeError(f"{source.name} 返回空{method}，继续回退")
                self.health[source.name] = min(100, self.health[source.name] + 5)
                return result, source.name
            except _NonRetryableError as e:
                logger.error(f"[{source.name}] 认证失败，跳过此源：{e}")
                self.health[source.name] = 0
                last_err = e
            except Exception as e:
                last_err = e
                self.health[source.name] = max(0, self.health[source.name] - 30)
                logger.warning(f"[{source.name}] {method}({args[0] if args else ''}) 失败：{e}")
        raise RuntimeError(f"所有数据源均失败：{last_err}")

    @property
    def active_name(self) -> str:
        try:
            return self._best().name
        except Exception:
            return "none"


# ── 初始化全局路由器 ─────────────────────────────────────
_MASSIVE_POOL = None
_SOURCES = []
ROUTER = None
_ALPACA_PAPER_SOURCE = None
_CBOE_SOURCE = None
OPTIONS_SOURCES = []
OPTIONS_ROUTER = None


def _auto_workers() -> int:
    if ARGS.workers:
        return ARGS.workers
    # 默认降并发，降低 CBOE/上游限流概率
    return 2

def _apply_args(args: argparse.Namespace):
    global ARGS, RISK_FREE_RATE, DELTA_MIN, DELTA_MAX
    global DTE_PREFERRED_MIN, DTE_PREFERRED_MAX, DTE_FALLBACK_MIN, DTE_FALLBACK_MAX
    global KEEP_FILES, MAX_SPREAD_PCT, MIN_OI
    ARGS = args
    RISK_FREE_RATE = ARGS.risk_free
    DELTA_MIN = ARGS.delta_min
    DELTA_MAX = ARGS.delta_max
    DTE_PREFERRED_MIN = ARGS.dte_min
    DTE_PREFERRED_MAX = ARGS.dte_max
    DTE_FALLBACK_MIN = ARGS.dte_fb_min
    DTE_FALLBACK_MAX = ARGS.dte_fb_max
    KEEP_FILES = ARGS.keep_files
    MAX_SPREAD_PCT = ARGS.max_spread
    MIN_OI = ARGS.min_oi


def _init_runtime():
    global _MASSIVE_POOL, _SOURCES, ROUTER
    global _ALPACA_PAPER_SOURCE, _CBOE_SOURCE, OPTIONS_SOURCES, OPTIONS_ROUTER, MAX_WORKERS
    _MASSIVE_POOL = MassiveKeyPool(MASSIVE_KEYS_LIST, min_gap=ARGS.massive_gap)
    massive_source = MassiveSource(_MASSIVE_POOL, MASSIVE_BASE_URL, MASSIVE_ENDPOINTS)
    tradier_source = TradierSource(TRADIER_TOKEN, TRADIER_SANDBOX)
    polygon_source = PolygonSource(POLYGON_API_KEY)
    yfinance_source = YFinanceSource()

    def _massive_host_resolvable() -> bool:
        try:
            host = MASSIVE_BASE_URL.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0]
            if not host:
                return False
            socket.getaddrinfo(host, 443)
            return True
        except Exception:
            return False

    massive_ok = _massive_host_resolvable()
    if not massive_ok:
        logger.warning(
            f"Massive 域名不可解析，自动降级顺序：{MASSIVE_BASE_URL}（将置后，优先其他数据源）"
        )

    # Massive 可解析时保持在前；不可解析时自动降级到末位，避免每次调用先报 DNS 错误
    _SOURCES = (
        [massive_source, tradier_source, polygon_source, yfinance_source]
        if massive_ok
        else [tradier_source, polygon_source, yfinance_source, massive_source]
    )
    ROUTER = DataRouter(_SOURCES, preferred=ARGS.source)
    _ALPACA_PAPER_SOURCE = AlpacaPaperSource(
        ALPACA_PAPER_KEY, ALPACA_PAPER_SECRET,
        paper_base_url=ALPACA_PAPER_BASE_URL,
        data_base_url=ALPACA_DATA_BASE_URL,
    )
    _CBOE_SOURCE = CboeSource(request_gap_sec=ARGS.cboe_gap)
    # 期权链同样降噪：Massive DNS 不可解析时放到备用末位
    options_tail = (
        [massive_source, tradier_source, polygon_source, yfinance_source]
        if massive_ok
        else [tradier_source, polygon_source, yfinance_source, massive_source]
    )
    if ARGS.disable_cboe:
        OPTIONS_SOURCES = [_ALPACA_PAPER_SOURCE] + options_tail
        OPTIONS_ROUTER = DataRouter(OPTIONS_SOURCES, preferred="alpaca")
    else:
        OPTIONS_SOURCES = [_CBOE_SOURCE, _ALPACA_PAPER_SOURCE] + options_tail
        OPTIONS_ROUTER = DataRouter(OPTIONS_SOURCES, preferred="cboe")
    MAX_WORKERS = _auto_workers()


MAX_WORKERS = 4


# ═══════════════════════════════════════════════════════════
#  SQLite 真实 IV 历史库
# ═══════════════════════════════════════════════════════════

IV_DB_PATH = Path(__file__).parent / "iv_history.db"

def _init_iv_db():
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

_init_iv_db()


def _iv_history_health_check(universe_n: Optional[int] = None):
    """
    启动自检：统计 IV 历史库近 30 天内有数据的标的数量。
    若覆盖率 < 50%，提示本轮扫描可能大量退化为 HV 代理。
    universe_n：本轮计划扫描的标的数；默认 len(TICKERS)。
    """
    if not IV_DB_PATH.exists():
        print("\n" + "=" * 72)
        print("⚠️  IV 历史库不存在 —— 所有 IVR 将使用 HV 代理")
        print("    本轮信号可信度显著下降，建议先建库再决策")
        print("=" * 72 + "\n")
        logger.warning("IV 历史库不存在")
        return

    try:
        conn = sqlite3.connect(str(IV_DB_PATH))
        cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(DISTINCT ticker) FROM iv_snapshots WHERE snap_date >= ?",
            (cutoff,),
        )
        fresh = cur.fetchone()[0] or 0
        conn.close()
        total = universe_n if universe_n is not None else len(TICKERS)
        if total > 0 and fresh < total * 0.5:
            print("\n" + "=" * 72)
            print(f"⚠️  IV 历史库近 30 天覆盖率不足：{fresh}/{total} 标的")
            print("    大多数 IVR 将走 HV 代理，本轮信号可信度低")
            print("    请检查 save_iv_snapshot 与 calculate_ivr 调用链路")
            print("=" * 72 + "\n")
            logger.warning(f"IV 库覆盖率不足: {fresh}/{total}")
    except Exception as e:
        logger.warning(f"IV 库健康检查失败: {e}")


def save_iv_snapshot(ticker: str, iv_atm: float, source: str):
    """保存 ATM IV 快照（同一天内保留最新一条）"""
    if not iv_atm or iv_atm <= 0:
        return
    today = datetime.now().strftime("%Y-%m-%d")
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = sqlite3.connect(IV_DB_PATH)
        conn.execute("DELETE FROM iv_snapshots WHERE ticker=? AND snap_date=?", (ticker, today))
        conn.execute(
            "INSERT INTO iv_snapshots (ticker, snap_date, snap_ts, iv_atm, source) VALUES (?,?,?,?,?)",
            (ticker, today, ts, iv_atm, source)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"IV历史写入失败 {ticker}: {e}")


def load_iv_history(ticker: str, days: int = 365):
    try:
        conn   = sqlite3.connect(IV_DB_PATH)
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows   = conn.execute(
            "SELECT snap_date, iv_atm FROM iv_snapshots "
            "WHERE ticker=? AND snap_date>=? ORDER BY snap_date",
            (ticker, cutoff)
        ).fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def get_ivr_5d_ago(ticker: str) -> Optional[float]:
    """
    计算约 5 个交易日前的 IVR（Opportunity Alert 条件2：IVR 涨幅检测）。
    使用与当前IVR相同的历史高低区间，取倒数第6条记录作为5日前基准。
    需要SQLite IV历史库积累≥10条记录才能计算，否则返回None。
    """
    rows   = load_iv_history(ticker, days=365)
    values = [r[1] for r in rows if r[1] and r[1] > 0]
    if len(values) < 10:
        return None
    high = max(values)
    low  = min(values)
    if high == low:
        return None
    # values[-1]是最新，向前数6位约为5个交易日前
    idx_5d  = max(len(values) - 6, 0)
    past_iv = values[idx_5d]
    return round((past_iv - low) / (high - low) * 100, 1)


def evaluate_opportunity_alert(
    stock: dict,
    ivr: Optional[float],
    ivr_5d_ago: Optional[float],
    in_blackout: bool,
    in_macro_blackout: bool,
    grade: str,
    spx_not_new_20d_low: bool,
) -> dict:
    """
    评估 Opportunity Alert 五项触发条件。
    in_macro_blackout：FOMC / CPI / NFP / BOJ 合并黑名单窗口。

    五项条件：
      C1  5日跌幅 ≥ 分档阈值 OR 3日跌幅 ≥ 分档阈值（跌幅触发）
          分档阈值：A+/A → 5日≥5% OR 3日≥3%
                   B   → 5日≥6% OR 3日≥4%
                   C   → 5日≥12% OR 3日≥8%
      C2  IVR较5日前上升 ≥ 15个百分点
      C3  无财报 / 宏观事件黑名单（FOMC+CPI+NFP+BOJ 合并窗口）
      C4  标的评级 ≥ B（C级高波标的不触发——已由C4过滤，C级保留高阈值参与C1）
      C5  SPX未创近20日新低（非系统性下跌）

    全部满足 → triggered=True，建议仓位×50%走正常开仓流程。
    仅C1满足但其他未全满足 → partial=True，供人工参考。
    """
    # 分档阈值表：(5日门槛%, 3日门槛%)
    THRESHOLDS = {
        "A+": (5.0, 3.0),
        "A":  (5.0, 3.0),
        "B":  (6.0, 4.0),
        "C":  (12.0, 8.0),
    }
    thresh_5d, thresh_3d = THRESHOLDS.get(grade, (6.0, 4.0))

    drop_5d = stock.get("drop_5d_pct")
    drop_3d = stock.get("drop_3d_pct")
    consec_down = stock.get("consecutive_down_days", 0)  # 保留为辅助参考

    # C1：跌幅达标（负数=下跌，所以用<=负阈值）
    c1_5d = (drop_5d is not None) and (drop_5d <= -thresh_5d)
    c1_3d = (drop_3d is not None) and (drop_3d <= -thresh_3d)
    c1    = c1_5d or c1_3d

    # C2：IVR 5日涨幅
    ivr_delta: Optional[float] = None
    c2 = False
    if ivr is not None and ivr_5d_ago is not None:
        ivr_delta = round(ivr - ivr_5d_ago, 1)
        c2 = ivr_delta >= 15

    c3 = not in_blackout and not in_macro_blackout
    c4 = grade in ("A+", "A", "B")
    c5 = spx_not_new_20d_low

    triggered = c1 and c2 and c3 and c4 and c5
    partial   = c1 and not triggered  # C1满足但被其他条件过滤，供参考

    return {
        "triggered":    triggered,
        "partial":      partial,
        "consec_down":  consec_down,    # 辅助参考，不再是触发条件
        "drop_5d_pct":  drop_5d,
        "drop_3d_pct":  drop_3d,
        "ivr_delta_5d": ivr_delta,
        "thresh_5d":    thresh_5d,
        "thresh_3d":    thresh_3d,
        "conds": {
            "c1_drop5d":  c1_5d,
            "c1_drop3d":  c1_3d,
            "c1":         c1,
            "c2_ivr15":   c2,
            "c3_no_bl":   c3,
            "c4_grade_b": c4,
            "c5_spx_ok":  c5,
        },
    }




def calculate_real_ivr(ticker: str):
    """基于真实 IV 历史计算 IVR，同时计算 IV 趋势"""
    rows = load_iv_history(ticker, days=365)
    if len(rows) < 30:
        return None, None, None, None, f"hv_proxy（真实历史仅{len(rows)}条，需≥30）", "unknown"

    values = [r[1] for r in rows if r[1] and r[1] > 0]
    if not values:
        return None, None, None, None, "hv_proxy（无有效IV值）", "unknown"

    curr = values[-1]
    high = max(values)
    low  = min(values)

    # IV 趋势：最近 5 条 vs 前 5 条
    if len(values) >= 10:
        recent_avg = sum(values[-5:]) / 5
        prior_avg  = sum(values[-10:-5]) / 5
        if prior_avg > 0:
            iv_trend = ("rising"  if recent_avg > prior_avg * 1.05 else
                        "falling" if recent_avg < prior_avg * 0.95 else "flat")
        else:
            iv_trend = "unknown"
    else:
        iv_trend = "unknown"

    if high == low:
        return None, curr, high, low, "real_iv（区间为零）", iv_trend

    ivr = (curr - low) / (high - low) * 100
    return round(ivr, 1), round(curr, 1), round(high, 1), round(low, 1), "real_iv ✓", iv_trend


def calculate_ivr(ticker: str):
    """
    IVR 计算：真实 IV 历史优先，HV 代理兜底。
    返回：(ivr, current_iv, high, low, source_label, iv_trend)
    第 6 个返回值：iv_trend
    """
    ivr, curr, high, low, label, iv_trend = calculate_real_ivr(ticker)
    if ivr is not None:
        return ivr, curr, high, low, label, iv_trend
    try:
        hist, _ = ROUTER.call("get_history", ticker, IVR_HISTORY_PERIOD)
        if hist is None or len(hist) < HV_WINDOW + 10:
            return None, None, None, None, "hv_proxy（数据不足）", "unknown"
        log_ret   = np.log(hist["Close"] / hist["Close"].shift(1)).dropna()
        hv_series = log_ret.rolling(HV_WINDOW).std() * math.sqrt(252) * 100
        hv_series = hv_series.dropna()
        if hv_series.empty:
            return None, None, None, None, "hv_proxy（计算失败）", "unknown"
        c = float(hv_series.iloc[-1])
        h = float(hv_series.max())
        l = float(hv_series.min())
        if h == l:
            return None, c, h, l, "hv_proxy（区间为零）", "unknown"
        ivr = (c - l) / (h - l) * 100
        return round(ivr, 1), round(c, 1), round(h, 1), round(l, 1), "hv_proxy ⚠（非真实IVR）", "unknown"
    except Exception as e:
        logger.warning(f"{ticker} IVR计算异常：{e}")
        return None, None, None, None, "hv_proxy（异常）", "unknown"


# ═══════════════════════════════════════════════════════════
#  VIX 与 SP500
# ═══════════════════════════════════════════════════════════

def get_vix() -> dict:
    result = {"vix": None, "vix3m": None, "term_structure": None, "source": None}
    for index_code, key in [("_VIX", "vix"), ("_VIX3M", "vix3m")]:
        try:
            r = requests.get(
                f"https://cdn.cboe.com/api/global/delayed_quotes/options/{index_code}.json",
                timeout=8, headers={"User-Agent": "Mozilla/5.0"}
            )
            r.raise_for_status()
            data  = r.json()
            price = (data.get("data", {}).get("current_price")
                     or data.get("data", {}).get("close"))
            if price:
                result[key]      = round(float(price), 2)
                result["source"] = result.get("source") or "cboe_official"
        except Exception as e:
            if key == "vix":
                logger.warning(f"CBOE {index_code} 接口失败：{e}")

    if result["vix"] is None:
        try:
            hist, src = ROUTER.call("get_history", "^VIX", "5d")
            if hist is not None and not hist.empty:
                result["vix"]    = round(float(hist["Close"].iloc[-1]), 2)
                result["source"] = f"router:{src}"
        except Exception as e:
            logger.error(f"VIX 全部数据源失败：{e}")

    if result["vix3m"] is None and result["vix"] is not None:
        try:
            hist3, _ = ROUTER.call("get_history", "^VIX3M", "5d")
            if hist3 is not None and not hist3.empty:
                result["vix3m"] = round(float(hist3["Close"].iloc[-1]), 2)
        except Exception:
            pass

    if result["vix"] and result["vix3m"]:
        result["term_structure"] = "INVERTED" if result["vix"] > result["vix3m"] else "NORMAL"

    return result


def get_sp500() -> Optional[dict]:
    try:
        hist, _ = ROUTER.call("get_history", "^GSPC", "1y")
        if hist is None or hist.empty:
            return None
        close    = float(hist["Close"].iloc[-1])
        ma200    = float(hist["Close"].rolling(200).mean().iloc[-1]) if len(hist) >= 200 else None
        # 窗口排除当天本身：判断“今天是否创过去20个交易日新低”
        peak     = float(hist["Close"].iloc[-21:-1].max()) if len(hist) >= 21 else close
        low_20d  = float(hist["Close"].iloc[-21:-1].min()) if len(hist) >= 21 else close
        drawdown = round((peak - close) / peak * 100, 2) if peak > 0 else 0
        return {
            "price":        round(close, 2),
            "ma200":        round(ma200, 2) if ma200 else None,
            "above_ma200":  (close > ma200) if ma200 else None,
            "drawdown_20d": drawdown,
            "low_20d":      round(low_20d, 2),
            "at_20d_low":   close < low_20d,  # 严格小于才算“创20日新低”
        }
    except Exception as e:
        logger.warning(f"S&P500获取失败: {e}")
        return None


# ═══════════════════════════════════════════════════════════
#  Pre-screen Gate（5项）
# ═══════════════════════════════════════════════════════════

class GateResult:
    def __init__(self, passed: bool, name: str, reason: str,
                 delta_adj: float = 0.0, score: Optional[int] = None,
                 skipped: bool = False):
        self.passed    = passed
        self.name      = name
        self.reason    = reason
        self.delta_adj = delta_adj
        self.score     = score
        self.skipped   = skipped


def run_pre_screen_gate(vix_data: dict, sp500: Optional[dict], margin_used: Optional[float]):
    """
    返回：(all_passed, delta_adj, [GateResult, ...])

    Gate-3 倒挂时：delta_adj 为正值，收紧 DELTA_MIN（排除最激进 Delta 端），
    在 process_ticker 中与 DELTA_MIN 相加生效。
    """
    results   = []
    delta_adj = 0.0
    vix       = vix_data.get("vix")
    vix3m     = vix_data.get("vix3m")
    ts        = vix_data.get("term_structure")

    # Gate-1：市场状态
    if sp500 is None:
        results.append(GateResult(True, "Gate-1 市场状态", "无S&P500数据，跳过（默认通过）",
                                  skipped=True))
    else:
        above    = sp500.get("above_ma200")
        drawdown = sp500.get("drawdown_20d", 0)
        if above is False and drawdown > 10:
            results.append(GateResult(False, "Gate-1 市场状态",
                                      f"SPY在MA200下方且20日回撤{drawdown}%>10%，市场D级"))
        else:
            pos = "上方" if above else "下方（⚠）"
            results.append(GateResult(True, "Gate-1 市场状态",
                                      f"SPY在MA200{pos}，20日回撤{drawdown}%"))

    # Gate-2：VIX ≤ 35
    if vix is None:
        results.append(GateResult(True, "Gate-2 VIX", "无VIX数据，跳过（默认通过）",
                                  skipped=True))
    elif vix > 35:
        results.append(GateResult(False, "Gate-2 VIX",
                                  f"VIX={vix} > 35，波动率过高，禁止卖出期权"))
    else:
        results.append(GateResult(True, "Gate-2 VIX", f"VIX={vix} ≤ 35 ✓"))

    # Gate-3：VIX 期限结构（倒挂 → 收紧 DELTA_MIN）
    if ts == "INVERTED":
        delta_adj = 0.02   # 正值：例如 DELTA_MIN -0.30 → -0.28
        results.append(GateResult(True, "Gate-3 期限结构",
                                  f"倒挂 VIX({vix})>VIX3M({vix3m})，"
                                  f"DELTA_MIN从{DELTA_MIN}收紧至{DELTA_MIN+delta_adj:.2f}（排除最激进端）",
                                  delta_adj=delta_adj))
    else:
        msg = (f"NORMAL（VIX {vix}, VIX3M {vix3m}）" if ts else "期限结构数据不全，默认正常")
        results.append(GateResult(True, "Gate-3 期限结构", msg))

    # Gate-4：保证金使用率 < 55%
    if margin_used is None:
        results.append(GateResult(True, "Gate-4 保证金",
                                  "未提供（用 --margin-used 传入），跳过检查",
                                  skipped=True))
    elif margin_used >= 55:
        results.append(GateResult(False, "Gate-4 保证金",
                                  f"使用率{margin_used}% ≥ 55%，禁止新开仓"))
    else:
        results.append(GateResult(True, "Gate-4 保证金", f"{margin_used}% < 55% ✓"))

    # Gate-5：三因子得分 ≥ 50
    score = 0
    score += (25 if vix is None else 35 if vix < 20 else 25 if vix < 28 else 15 if vix < 35 else 0)
    if sp500:
        score += (30 if sp500.get("above_ma200") else 15 if sp500.get("drawdown_20d", 0) < 5 else 5)
    else:
        score += 15
    score += 25 if ts == "NORMAL" else (15 if ts is None else 5)

    if score < 50:
        results.append(GateResult(False, "Gate-5 三因子",
                                  f"得分={score} < 50，市场综合环境不佳", score=score))
    else:
        results.append(GateResult(True, "Gate-5 三因子",
                                  f"得分={score} ≥ 50 ✓", score=score))

    all_passed = all(g.passed for g in results)
    return all_passed, delta_adj, results


# ═══════════════════════════════════════════════════════════
#  Greeks（Black-Scholes，无真实 Greeks 时使用）
# ═══════════════════════════════════════════════════════════

def norm_cdf(x):
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

def norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def bs_put_delta(S, K, T, r, sigma) -> Optional[float]:
    """
    sigma 为小数形式（0.30 = 30%）；调用前在 get_put_chain 中已归一化。
    """
    if not all(v and v > 0 for v in [S, K, T, sigma]):
        return None
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        return round(norm_cdf(d1) - 1, 3)
    except Exception:
        return None

def bs_put_theta(S, K, T, r, sigma) -> Optional[float]:
    if not all(v and v > 0 for v in [S, K, T, sigma]):
        return None
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        theta = (-(S * norm_pdf(d1) * sigma) / (2 * math.sqrt(T))
                 + r * K * math.exp(-r * T) * norm_cdf(-d2))
        return round(theta / 365, 4)
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════
#  个股数据获取
# ═══════════════════════════════════════════════════════════

@retry(max_attempts=3, delay=2)
def get_stock_data(ticker: str) -> Optional[dict]:
    hist, src = ROUTER.call("get_history", ticker, "1y")
    if hist is None or hist.empty:
        return None

    close = float(hist["Close"].iloc[-1])
    ma20  = float(hist["Close"].rolling(20).mean().iloc[-1])  if len(hist) >= 20  else None
    ma50  = float(hist["Close"].rolling(50).mean().iloc[-1])  if len(hist) >= 50  else None
    ma200 = float(hist["Close"].rolling(200).mean().iloc[-1]) if len(hist) >= 200 else None
    h52   = float(hist["High"].max())
    l52   = float(hist["Low"].min())
    pct52 = round((close - l52) / (h52 - l52) * 100, 1) if h52 != l52 else None

    earnings_date    = _get_earnings_date(ticker)
    days_to_earnings = None
    if earnings_date and earnings_date != "N/A":
        try:
            ed = datetime.strptime(earnings_date[:10], "%Y-%m-%d").date()
            days_to_earnings = (ed - datetime.now().date()).days
        except Exception:
            pass

    # 连跌天数（辅助参考）
    closes_list = hist["Close"].tolist()
    consec_down = 0
    for i in range(len(closes_list) - 1, 0, -1):
        if closes_list[i] < closes_list[i - 1]:
            consec_down += 1
        else:
            break
    consec_down = min(consec_down, 10)

    # 5日/3日跌幅（Opportunity Alert C1）
    def _pct_change(closes, n_days_back):
        """取倒数第n_days_back+1条收盘价计算跌幅，数据不足时返回None"""
        idx = -(n_days_back + 1)
        if len(closes) >= abs(idx):
            past = closes[idx]
            if past and past > 0:
                return round((closes[-1] / past - 1) * 100, 2)
        return None

    drop_5d_pct = _pct_change(closes_list, 5)
    drop_3d_pct = _pct_change(closes_list, 3)

    return {
        "price":                round(close, 2),
        "ma20":                 round(ma20,  2) if ma20  else None,
        "ma50":                 round(ma50,  2) if ma50  else None,
        "ma200":                round(ma200, 2) if ma200 else None,
        "above_ma20":           close > ma20    if ma20  else None,
        "above_ma50":           close > ma50    if ma50  else None,
        "above_ma200":          close > ma200   if ma200 else None,
        "high_52w":             round(h52, 2),
        "low_52w":              round(l52, 2),
        "price_percentile_52w": pct52,
        "next_earnings":        earnings_date,
        "days_to_earnings":     days_to_earnings,
        "consecutive_down_days": consec_down,    # 辅助参考
        "drop_5d_pct":          drop_5d_pct,     # 5日涨跌幅%（负=下跌）
        "drop_3d_pct":          drop_3d_pct,     # 3日涨跌幅%
        "data_source":          src,
    }


def _get_earnings_date(ticker: str) -> str:
    """财报日：仅 yfinance 支持，其他源返回 N/A"""
    if not _YF_AVAILABLE:
        return "N/A"
    try:
        tk  = yf.Ticker(ticker)
        cal = tk.calendar
        if cal is not None:
            if isinstance(cal, dict):
                dates = cal.get("Earnings Date") or cal.get("earningsDate")
                if dates:
                    d = dates[0] if isinstance(dates, list) else dates
                    return str(d)[:10]
            elif hasattr(cal, "columns") and "Earnings Date" in cal.columns:
                return str(cal["Earnings Date"].iloc[0])[:10]
    except Exception:
        pass
    try:
        info = yf.Ticker(ticker).info or {}
        for k in ["earningsDate", "nextEarningsDate", "earningsTimestamp"]:
            if info.get(k):
                v = info[k]
                return (datetime.fromtimestamp(v).strftime("%Y-%m-%d")
                        if isinstance(v, (int, float)) else str(v)[:10])
    except Exception:
        pass
    return "N/A"


# ═══════════════════════════════════════════════════════════
#  到期日选择（月度优先于周度）
# ═══════════════════════════════════════════════════════════

def select_expiry(options_dates: list) -> Optional[dict]:
    today                       = datetime.now().date()
    weekly, monthly, fallback   = [], [], []

    for ds in options_dates:
        try:
            exp = datetime.strptime(ds, "%Y-%m-%d").date()
        except Exception:
            continue
        dte        = (exp - today).days
        is_friday  = (exp.weekday() == 4)
        is_monthly = False
        if is_friday:
            fd = exp.replace(day=1)
            ff = fd + timedelta(days=(4 - fd.weekday()) % 7)
            is_monthly = (exp == ff + timedelta(weeks=2))

        if DTE_PREFERRED_MIN <= dte <= DTE_PREFERRED_MAX:
            e = {"date": ds, "dte": dte}
            if is_monthly:
                monthly.append(e)
            elif is_friday:
                weekly.append({**e, "is_monthly": False})
        if DTE_FALLBACK_MIN <= dte <= DTE_FALLBACK_MAX:
            fallback.append({"date": ds, "dte": dte})

    # 月度优先（流动性最佳）
    if monthly:
        best = min(monthly, key=lambda x: abs(x["dte"] - 40))
        return {"date": best["date"], "dte": best["dte"], "type": "preferred_monthly",
                "note": f"月度(第三周五)，DTE={best['dte']}天"}
    if weekly:
        best = min(weekly, key=lambda x: abs(x["dte"] - 40))
        return {"date": best["date"], "dte": best["dte"], "type": "preferred_weekly",
                "note": f"周到期(Weekly)，DTE={best['dte']}天"}
    if fallback:
        best = min(fallback, key=lambda x: abs(x["dte"] - 40))
        return {"date": best["date"], "dte": best["dte"], "type": "fallback",
                "note": f"⚠ 保底，DTE={best['dte']}天"}
    return None


# ═══════════════════════════════════════════════════════════
#  期权链获取与筛选
# ═══════════════════════════════════════════════════════════

@retry(max_attempts=3, delay=2)
def get_put_chain(ticker: str, expiry: str, price: float,
                  dte: int, otm_buffer: float, atm_iv_ref: list) -> list:
    """获取 Put 期权链并完成质量过滤（零价 bid/ask 跳过；BS 前 sigma 归一化）。"""
    puts, src = OPTIONS_ROUTER.call("get_option_chain", ticker, expiry)
    if puts is None or puts.empty:
        return []

    # 宽筛行权价：OTM buffer 下方，不超过buffer×2倍深度
    max_k = price * (1 - otm_buffer * 0.5)
    min_k = price * (1 - otm_buffer * 2.0)
    filt  = puts[(puts["strike"] <= max_k) & (puts["strike"] >= min_k)].copy()
    if filt.empty:
        filt = puts[puts["strike"] < price].tail(15).copy()

    T       = dte / 365.0
    results = []

    for _, row in filt.iterrows():
        strike = float(row.get("strike",     0))
        bid    = float(row.get("bid",        0) or 0)
        ask    = float(row.get("ask",        0) or 0)
        last   = float(row.get("lastPrice",  0) or 0)
        oi     = int(row.get("openInterest", 0) or 0)
        vol    = int(row.get("volume",       0) or 0)
        iv_raw = float(row.get("impliedVolatility", 0) or 0)
        has_rg = bool(row.get("has_real_greeks", False))

        # ── 质量过滤 ─────────────────────────────────────
        if iv_raw <= 0:
            continue

        # bid/ask 零价过滤
        if bid <= 0 or ask <= 0:
            continue

        mid        = round((bid + ask) / 2, 2)
        spread_pct = round((ask - bid) / mid * 100, 2)
        if spread_pct > MAX_SPREAD_PCT:
            continue
        if oi < MIN_OI:
            continue
        # ── 过滤完成 ─────────────────────────────────────

        otm_pct = round((price - strike) / price * 100, 2)

        # Greeks：优先真实值，否则 BS 估算
        if has_rg and row.get("delta") not in (0, 0.0, None):
            delta        = round(float(row["delta"]), 3)
            theta_raw    = float(row.get("theta", 0) or 0)
            theta_seller = round(-theta_raw * 100, 4) if theta_raw else None
            g_src        = "real"
        else:
            # BS sigma 归一化
            # iv_raw 来自 Polygon/yfinance 时是小数（0.30 = 30%）
            # 来自 Massive/部分源时是百分数（30.0 = 30%）
            # 统一归一化为小数后传入 BS 公式
            if iv_raw >= 1.0:
                iv_for_bs = iv_raw / 100.0   # 30.0 → 0.30
            else:
                iv_for_bs = iv_raw           # 0.30 → 0.30（已是小数）

            delta        = bs_put_delta(price, strike, T, RISK_FREE_RATE, iv_for_bs)
            theta_bs     = bs_put_theta(price, strike, T, RISK_FREE_RATE, iv_for_bs)
            theta_seller = round(-theta_bs * 100, 4) if theta_bs else None
            g_src        = "bs_estimated"

        ann_yield = round((mid / strike) * (365 / dte) * 100, 2) if (strike > 0 and dte > 0) else 0

        # 收集 ATM IV（最接近当前价格，用于历史库）
        if abs(strike - price) / price < 0.03 and iv_raw > 0:
            atm_iv_ref.append((abs(strike - price) / price, iv_raw, src))

        # IV 规范化（小数→百分比显示）
        iv_pct = round(iv_raw * 100, 2) if iv_raw < 5 else round(iv_raw, 2)

        results.append({
            "strike":           strike,
            "bid":              bid,
            "ask":              ask,
            "mid":              mid,
            "last":             last,
            "volume":           vol,
            "open_interest":    oi,
            "implied_vol_pct":  iv_pct,
            "otm_pct":          otm_pct,
            "spread_pct":       spread_pct,
            "delta":            delta,
            "theta_per_day":    theta_seller,
            "greeks_source":    g_src,
            "annualized_yield": ann_yield,
        })

    results.sort(key=lambda x: x["strike"], reverse=True)
    return results


def find_best_contracts(puts: list, ann_min: float, d_min: float, d_max: float,
                        otm_buffer_pct: float) -> list:
    """
    筛选符合 Delta 区间和年化收益要求的最优合约（最多 3 个）。

    排序：OTM% 深度优先，年化收益次之，流动性（OI）最后。
    """
    qualified = [
        p for p in puts
        if p.get("delta") is not None
        and d_min <= p["delta"] <= d_max
        and p.get("annualized_yield", 0) >= ann_min
        and p.get("otm_pct", 0) >= otm_buffer_pct * 100
    ]
    # OTM 深度优先，收益次之，流动性最后
    qualified.sort(key=lambda x: (
        x.get("otm_pct", 0),          # 主：OTM%越深越安全，降序
        x.get("annualized_yield", 0),  # 次：年化收益越高越好，降序
        x.get("open_interest", 0),     # 三：OI越大流动性越好，降序
    ), reverse=True)
    return qualified[:3]


# ═══════════════════════════════════════════════════════════
#  单标的处理（并发执行，线程安全）
# ═══════════════════════════════════════════════════════════

def process_ticker(symbol: str, config: dict, delta_adj: float, vix_val: Optional[float], sp500_data: Optional[dict] = None) -> tuple:
    """
    delta_adj：倒挂时 >0，与 DELTA_MIN 相加以收紧危险端（排除最激进 Delta）。
    sp500_data：由 scan_all 传入，用于 Opportunity Alert 条件5（SPX 未创 20 日新低）。
    """
    result = {"config": config, "status": "OK"}

    # 1. 股价 + 均线
    try:
        stock = get_stock_data(symbol)
    except Exception as e:
        logger.error(f"{symbol} 股价最终失败：{e}", exc_info=True)
        stock = None

    if not stock or not stock.get("price"):
        result.update({"status": "ERROR", "error": "无法获取价格"})
        return symbol, result

    price = stock["price"]

    # 2. IVR（含 iv_trend）
    ivr, curr_iv, iv_high, iv_low, ivr_label, iv_trend = calculate_ivr(symbol)

    is_hv_proxy = "hv_proxy" in (ivr_label or "")
    if ARGS.block_hv_proxy and is_hv_proxy:
        ivr_meets = False  # --block-hv-proxy：HV 代理直接不满足
    else:
        ivr_meets = (ivr >= config["ivr_min"]) if ivr is not None else None
        # None：IVR 未知（数据不足），不等同于通过
        # ivr_meets=None 时信号不产生（has_signal要求 is True）

    stock.update({
        "ivr": ivr, "ivr_label": ivr_label, "iv_trend": iv_trend,
        "current_iv": curr_iv, "iv_52w_high": iv_high, "iv_52w_low": iv_low,
        "ivr_meets_min": ivr_meets,
    })
    result["stock"] = stock

    # 3. 财报黑名单（日期缺失时保守处理）
    dte_to_earn = stock.get("days_to_earnings")
    blackout    = config["earnings_blackout"]
    earnings_na = (stock.get("next_earnings") == "N/A")
    earnings_unknown_risk = False

    if blackout > 0 and earnings_na:
        # 财报日获取失败且标的有财报黑名单要求 → 保守处理：视为在黑名单内
        in_blackout           = True
        earnings_unknown_risk = True
        logger.warning(f"{symbol} 财报日获取失败，保守处理为在黑名单内（earnings_blackout={blackout}天）")
    elif dte_to_earn is not None and blackout > 0:
        in_blackout = (0 <= dte_to_earn <= blackout)
    else:
        in_blackout = False

    near_blackout = (
        (not in_blackout)
        and dte_to_earn is not None
        and blackout > 0
        and dte_to_earn >= 0
        and dte_to_earn <= blackout + NEAR_BLACKOUT_BUFFER
    )
    post_earnings_vol = bool(
        dte_to_earn is not None and (-POST_EARNINGS_VOL_DAYS < dte_to_earn < 0)
    )

    fomc_blackout = int(config.get("fomc_blackout", 0) or 0)
    dte_to_fomc = _days_to_next_fomc()
    in_fomc_blackout = bool(
        fomc_blackout > 0 and dte_to_fomc is not None and 0 <= dte_to_fomc <= fomc_blackout
    )

    cpi_b = int(config.get("cpi_blackout", 0) or 0)
    nfp_b = int(config.get("nfp_blackout", 0) or 0)
    boj_b = int(config.get("boj_blackout", 0) or 0)
    dte_to_cpi = _days_to_next_cpi()
    dte_to_nfp = _days_to_next_nfp()
    dte_to_boj = _days_to_next_boj()
    in_cpi_blackout = bool(
        cpi_b > 0 and dte_to_cpi is not None and 0 <= dte_to_cpi <= cpi_b
    )
    in_nfp_blackout = bool(
        nfp_b > 0 and dte_to_nfp is not None and 0 <= dte_to_nfp <= nfp_b
    )
    in_boj_blackout = bool(
        boj_b > 0 and dte_to_boj is not None and 0 <= dte_to_boj <= boj_b
    )
    in_macro_blackout = bool(
        in_fomc_blackout or in_cpi_blackout or in_nfp_blackout or in_boj_blackout
    )

    cc_rule = str(config.get("cc_rule") or "").strip()
    cc_warnings = [f"cc:{cc_rule}"] if cc_rule else []

    result["in_earnings_blackout"] = in_blackout
    result["earnings_unknown_risk"] = earnings_unknown_risk
    result["near_earnings_blackout"] = near_blackout
    result["post_earnings_vol"] = post_earnings_vol
    result["in_fomc_blackout"] = in_fomc_blackout
    result["days_to_fomc"] = dte_to_fomc
    result["in_cpi_blackout"] = in_cpi_blackout
    result["in_nfp_blackout"] = in_nfp_blackout
    result["in_boj_blackout"] = in_boj_blackout
    result["days_to_cpi"] = dte_to_cpi
    result["days_to_nfp"] = dte_to_nfp
    result["days_to_boj"] = dte_to_boj
    result["in_macro_blackout"] = in_macro_blackout
    result["cc_warnings"] = cc_warnings

    # Opportunity Alert
    ivr_5d_ago = get_ivr_5d_ago(symbol)
    spx_not_new_20d_low = True  # 默认通过（无数据时保守允许）
    if sp500_data:
        spx_not_new_20d_low = not sp500_data.get("at_20d_low", False)
    opp_alert = evaluate_opportunity_alert(
        stock       = stock,
        ivr         = ivr,
        ivr_5d_ago  = ivr_5d_ago,
        in_blackout = in_blackout,
        in_macro_blackout = in_macro_blackout,
        grade       = config.get("grade", "C"),
        spx_not_new_20d_low = spx_not_new_20d_low,
    )
    result["opportunity_alert"] = opp_alert

    # 4. 期权到期日
    try:
        opt_dates, _ = OPTIONS_ROUTER.call("get_option_dates", symbol)
    except Exception as e:
        logger.error(f"{symbol} 期权日期失败：{e}", exc_info=True)
        result.update({"status": "ERROR", "error": f"无期权数据: {e}"})
        return symbol, result

    exp_info = select_expiry(opt_dates or [])
    if not exp_info:
        result.update({"status": "NO_EXPIRY",
                       "error": f"无合适到期日（DTE {DTE_FALLBACK_MIN}-{DTE_FALLBACK_MAX}）"})
        return symbol, result

    exp_date = exp_info["date"]
    dte      = exp_info["dte"]

    # 5. 期权链
    atm_iv_ref = []
    try:
        puts = get_put_chain(symbol, exp_date, price, dte, config["otm_buffer"], atm_iv_ref)
    except Exception as e:
        logger.error(f"{symbol} 期权链最终失败：{e}", exc_info=True)
        puts = []

    # 保存 ATM IV 到历史库
    if atm_iv_ref:
        best_atm = min(atm_iv_ref, key=lambda x: x[0])
        save_iv_snapshot(symbol, best_atm[1], best_atm[2])

    # Delta 范围：倒挂时收紧 DELTA_MIN（危险端）
    eff_d_min = DELTA_MIN + delta_adj   # e.g. -0.30 + 0.02 = -0.28（更严格，排除最激进）
    eff_d_max = DELTA_MAX               # -0.15 不变（安全端保留）

    bcs = find_best_contracts(puts, config["ann_min"], eff_d_min, eff_d_max, config["otm_buffer"])

    special_rules = config.get("special_rules") or []
    gld_vix_blocked = False
    if "gld_vix_gate" in special_rules and vix_val is not None and vix_val < 22:
        gld_vix_blocked = True

    # has_signal 要求 ivr_meets is True
    has_signal = (
        not in_blackout
        and not in_macro_blackout
        and not gld_vix_blocked
        and not (ARGS.block_post_earnings and post_earnings_vol)
        and ivr_meets is True           # None（IVR未知）不通过
        and len(bcs) > 0
    )

    result.update({
        "expiry":               exp_date,
        "dte":                  dte,
        "expiry_type":          exp_info["type"],
        "expiry_note":          exp_info["note"],
        "ivr_meets_threshold":  ivr_meets,
        "effective_delta_range": f"{eff_d_min:.2f} ~ {eff_d_max:.2f}",
        "puts_found":           len(puts),
        "best_contracts":       bcs,
        "gld_vix_blocked":      gld_vix_blocked,
        "has_signal":           has_signal,
    })
    return symbol, result


# ═══════════════════════════════════════════════════════════
#  摘要文件
# ═══════════════════════════════════════════════════════════

def write_summary(scan_results: dict, filepath: Path):
    tickers     = scan_results.get("tickers", {})
    signal_list = [(s, d) for s, d in tickers.items() if d.get("has_signal")]

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("=" * 72 + "\n")
        f.write(f"  OpenClaw 扫描摘要 {__version__}\n")
        f.write(f"  扫描时间：{scan_results.get('scan_time', 'N/A')}\n")
        f.write(f"  主数据源：{scan_results.get('data_source', 'N/A')}\n")
        f.write(f"  有信号标的：{len(signal_list)} / {len(tickers)}\n")
        cfg = scan_results.get("config", {})
        f.write(f"  DTE：{cfg.get('dte_preferred')}  Delta：{cfg.get('delta_range')}  "
                f"价差上限：{cfg.get('max_spread_pct')}%  最低OI：{cfg.get('min_oi')}\n")
        mkt = scan_results.get("market", {})
        sp500 = mkt.get("sp500") or {}
        f.write(f"  VIX：{mkt.get('vix')}  期限结构：{mkt.get('term_structure','N/A')}\n")
        f.write(f"  距下次FOMC：{mkt.get('days_to_fomc')} 天  "
                f"CPI：{mkt.get('days_to_cpi')} 天  NFP：{mkt.get('days_to_nfp')} 天  "
                f"BOJ：{mkt.get('days_to_boj')} 天\n")

        gates = scan_results.get("pre_screen_gates", [])
        gate_score = next((g.get("score") for g in gates
                           if "Gate-5" in str(g.get("name", ""))), None)
        gate4 = next((g for g in gates if "Gate-4" in str(g.get("name", ""))), None)
        if gate4:
            margin_input = scan_results.get("config", {}).get("margin_used")
            margin_verified = (not gate4.get("skipped", False)) and bool(gate4.get("passed"))
            margin_line = f"保证金：input={margin_input}%, verified={margin_verified} ({gate4.get('reason')})"
        else:
            margin_line = "保证金：N/A"
        f.write(f"  三因子得分：{gate_score}\n")
        f.write(f"  {margin_line}\n")
        f.write(f"  SP500 20日回撤：{sp500.get('drawdown_20d', 'N/A')}%\n")
        if gates:
            f.write("\n  Pre-screen Gate：\n")
            for g in gates:
                f.write(f"    {'✅' if g['passed'] else '❌'} {g['name']}: {g['reason']}\n")

        f.write("=" * 72 + "\n\n")

        if not signal_list:
            f.write("本次扫描无有信号标的。\n")
        else:
            f.write(f"{'标的':<6} {'级别':<4} {'现价':<9} {'DTE':<5} "
                    f"{'IVR%':<7} {'IV趋势':<8} {'IVR来源':<16} {'行权价':<8} "
                    f"{'Δ':<7} {'年化':<8} {'结构'}\n")
            f.write("-" * 96 + "\n")
            for sym, data in sorted(signal_list,
                                    key=lambda x: x[1].get("config",{}).get("grade","Z")):
                s  = data.get("stock", {})
                c0 = data.get("best_contracts", [{}])[0]
                g_icon = "📡" if c0.get("greeks_source") == "real" else "📐"
                iv_trend_str = s.get("iv_trend", "?")
                trend_icon = {"rising": "📈", "falling": "📉", "flat": "➡️"}.get(iv_trend_str, "❓")
                f.write(
                    f"{sym:<6} {data.get('config',{}).get('grade','?'):<4} "
                    f"${str(s.get('price','')):<8} {str(data.get('dte',''))+'天':<5} "
                    f"{str(s.get('ivr',''))+'%':<7} {trend_icon+iv_trend_str:<8} "
                    f"{str(s.get('ivr_label',''))[:14]:<16} "
                    f"{str(c0.get('strike',''))+'P':<8} "
                    f"Δ{str(c0.get('delta','')):<6} "
                    f"{str(c0.get('annualized_yield',''))+'%':<8} "
                    f"{data.get('config',{}).get('structure','CSP')} {g_icon}\n"
                )

        f.write("\n")
        f.write(f"输出说明（{__version__}）：\n")
        f.write("  📡=真实Greeks  📐=BS估算（sigma 已归一化）\n")
        f.write("  real_iv ✓=真实IVR  hv_proxy ⚠=HV代理\n")
        f.write("  IV趋势 📈rising / 📉falling / ➡️flat\n")
        f.write("  财报黑名单：获取失败时保守处理为在黑名单内\n")
        f.write("  IVR=None（数据不足）：不产生信号\n")
        f.write("  Delta倒挂调整：收紧DELTA_MIN（排除最激进端）\n")
        f.write("⚠ 本内容仅为数据参考，不构成任何投资建议。期权交易具有高风险。\n")


# ═══════════════════════════════════════════════════════════
#  LLM 精简输出
# ═══════════════════════════════════════════════════════════

def _compact_opp(opp: Optional[dict]) -> Optional[dict]:
    """将 opportunity_alert 压缩为 LLM 友好结构。"""
    if not opp:
        return None
    triggered = opp.get("triggered")
    partial   = opp.get("partial")
    if not triggered and not partial:
        return None  # 无关信息不输出，节省token
    out = {
        "t":     triggered,
        "cd":    opp.get("consec_down"),       # 连跌天数（辅助参考）
        "p5d":   opp.get("drop_5d_pct"),       # 5日跌幅%
        "p3d":   opp.get("drop_3d_pct"),       # 3日跌幅%
        "c1_5d": opp["conds"].get("c1_drop5d") if opp.get("conds") else None,
        "c1_3d": opp["conds"].get("c1_drop3d") if opp.get("conds") else None,
        "d5":    opp.get("ivr_delta_5d"),      # IVR 5日变化
    }
    if partial and not triggered:
        out["p"]    = True
        out["fail"] = [k for k, v in (opp.get("conds") or {}).items() if not v]
    # 过滤 None 值：d5 在历史数据不足时会是 None，避免 "d5":null 冗余输出
    return _drop_none(out)


def _drop_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def _ivt_short(ivt: Optional[str]) -> str:
    """IV 趋势压缩给 LLM：unknown→? rising→up falling→dn flat→fl"""
    m = {"unknown": "?", "rising": "up", "falling": "dn", "flat": "fl"}
    k = str(ivt).lower() if ivt else "unknown"
    return m.get(k, "?")


# 配置里的 structure 长中文串 → ASCII 短码（节省 token）
_STRUCTURE_SHORT = {
    "CSP": "csp",
    "Bull Put Spread": "bps",
    "强制Bull Put Spread": "fbps",
    "强制价差": "fspread",
    "CSP / C→价差": "csp_spread",
    "CSP（全天候）": "csp_aw",
    "CSP / C→×0.5": "csp_x05",
}


def _structure_short(s: Optional[str]) -> str:
    if not s:
        return "csp"
    if s in _STRUCTURE_SHORT:
        return _STRUCTURE_SHORT[s]
    t = s.strip()
    if "强制" in t and "价差" in t:
        return "fspread"
    if "Bull Put" in t:
        return "fbps" if "强制" in t else "bps"
    if t.startswith("CSP") and "价差" in t:
        return "csp_spread"
    if t.startswith("CSP"):
        return "csp"
    return "csp"


def _format_skip_drop(sym: str, opp: dict) -> Optional[str]:
    """无信号但有机遇提示时，单独写入 skip.drop，格式 SYM:tag(pct)。"""
    if not opp:
        return None
    if opp.get("triggered"):
        p5 = opp.get("drop_5d_pct")
        return f"{sym}:opp5d({p5}%)"
    if opp.get("partial"):
        conds = opp.get("conds") or {}
        if conds.get("c1_drop5d"):
            return f"{sym}:drop5d({opp.get('drop_5d_pct')}%)"
        if conds.get("c1_drop3d"):
            return f"{sym}:drop3d({opp.get('drop_3d_pct')}%)"
    return None


def build_llm_ready_json(raw_scan: dict) -> dict:
    """
    维护约定：
    - 只要本函数输出结构发生变更，必须同步 bump __version__
    - 并重新导出一次 schema 文档（openclaw_schema_vX.Y.md）供下游模型更新

    输出要点：
    1. 跳过标的：非 compact 时按原因短键分组（nc/ebl/ivrl/...）；compact 仍为 by_reason
    2. 默认仅保留最优合约；C级高波动标的保留 Top 2 便于人工比较
       （--full-output 时仍保留 Top 3）
    3. 缩短键名（tkr/px/dte/ivr/del/yld/otm/prem）
    4. IVR来源：real/hv 代替长字符串
    5. 新增 iv_trend、earn_warn 字段
    6. --compact-output：极简模式，跳过标的仅计数
    """
    tickers      = raw_scan.get("tickers", {})
    signals      = []
    skip_counts  = {}   # 跳过原因统计（与历史 by_reason 一致）
    skip_groups  = defaultdict(list)  # 按短键分组，非 compact 时写入 skip

    for sym, data in tickers.items():
        cfg        = data.get("config", {})
        stock      = data.get("stock", {})
        status     = data.get("status", "ERROR")
        has_signal = bool(data.get("has_signal"))
        dte        = data.get("dte", 0) or 0

        if status != "OK":
            reason = f"数据异常:{data.get('error','?')}"
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            if not ARGS.compact_output:
                err_s = str(data.get("error", "?")).replace(",", ";")[:120]
                skip_groups["err"].append(f"{sym}:{err_s}")
            continue

        if has_signal:
            grade = str(cfg.get("grade", "")).upper()
            # 默认模式：C级给2个候选便于人工比较，其余维持1个；full-output 保持原有Top3
            max_contracts = 3 if ARGS.full_output else (2 if grade == "C" else 1)
            bcs       = data.get("best_contracts", [])[:max_contracts]
            ivr_src   = "hv" if "hv_proxy" in str(stock.get("ivr_label", "")) else "real"
            iv_trend  = stock.get("iv_trend", "unknown")
            ivt_out   = _ivt_short(iv_trend)

            # 构建警告列表
            warnings = []
            if data.get("in_earnings_blackout"):
                warnings.append("earn_bl")
            if data.get("near_earnings_blackout"):
                warnings.append("near_blackout")
            if data.get("earnings_unknown_risk"):
                warnings.append("earn_unk")
            if data.get("post_earnings_vol"):
                warnings.append("post_earnings_vol")
            if data.get("in_fomc_blackout"):
                warnings.append("fomc_bl")
            if data.get("in_cpi_blackout"):
                warnings.append("cpi_bl")
            if data.get("in_nfp_blackout"):
                warnings.append("nfp_bl")
            if data.get("in_boj_blackout"):
                warnings.append("boj_bl")
            if data.get("gld_vix_blocked"):
                warnings.append("gld_vix_blocked")
            for cw in data.get("cc_warnings") or []:
                # 跳过 cc:xxx 类条目——cc 字段已单独输出，避免重复
                if cw and not str(cw).startswith("cc:"):
                    warnings.append(cw)
            if ivr_src == "hv":
                warnings.append("hv_proxy")
            if iv_trend == "rising":
                warnings.append("iv_rising")
            if iv_trend == "unknown":
                warnings.append("ivt_unknown")
            if ARGS.block_rising_iv and iv_trend == "rising":
                warnings.append("iv_rising_blocked")
            if not (DTE_PREFERRED_MIN <= dte <= DTE_PREFERRED_MAX):
                warnings.append(f"dte:{dte}")

            # 合约精简字段
            contracts_out = []
            for c in bcs:
                contracts_out.append({
                    "k":    c.get("strike"),                          # strike
                    "del":  round(float(c.get("delta", 0)), 2),       # delta（2位已足够）
                    "yld":  c.get("annualized_yield"),                 # annualized_yield%
                    "otm":  round(float(c.get("otm_pct", 0)), 2),     # otm%
                    "prem": round(float(c.get("mid", 0)), 2),          # premium
                    "spd":  round(float(c.get("spread_pct", 0)), 1)   # spread%
                    if c.get("spread_pct") is not None else None,
                    "oi":   c.get("open_interest"),
                    "vol":  c.get("volume"),
                    "gs":   "r" if c.get("greeks_source") == "real" else "bs",  # greeks_src
                    "str":  _structure_short(cfg.get("structure", "CSP")),
                })

            earn_days = stock.get("days_to_earnings")
            earn_note = None
            if isinstance(earn_days, (int, float)) and earn_days < 0:
                earn_note = f"post_earnings_{abs(int(earn_days))}d"
            # earn > 60 对当前 DTE 无操作价值，省略精确天数
            earn_out = None
            if isinstance(earn_days, (int, float)):
                earn_out = None if earn_days > 60 else earn_days

            # ivt 仅在已知趋势时才输出（unknown 已由 warn:ivt_unknown 覆盖）
            ivt_field = ivt_out if ivt_out != "?" else None

            sig_base = {
                "tkr":  sym,
                "g":    cfg.get("grade", "?"),
                "sec":  cfg.get("sector"),
                "cc":   (cfg.get("cc_rule") or None),
                "px":   stock.get("price"),
                "dte":  dte,
                "exp":  data.get("expiry"),
                "ivr":  stock.get("ivr"),
                "ivs":  ivr_src,
                "ivt":  ivt_field,
                "earn": earn_out,
                "earn_note": earn_note,
                "warn": warnings or None,
                "opp":  _compact_opp(data.get("opportunity_alert")),
                # th 仅在 full-output 时输出（LLM 从 SKILL 已知各标的阈值）
                "th":   {
                    "ivr_min": cfg.get("ivr_min"),
                    "ann_min": cfg.get("ann_min"),
                    "otm_buf": cfg.get("otm_buffer"),
                } if ARGS.full_output else None,
                "c":    contracts_out,
            }
            if cfg.get("official"):
                sig_base["off"] = True
            signal_item = _drop_none(sig_base)
            signals.append(signal_item)
            continue

        # 跳过原因分类（reason 用于 skip_counts；分组键用于压缩 skip 输出）
        mkt = raw_scan.get("market", {})
        if data.get("gld_vix_blocked"):
            vx = mkt.get("vix")
            reason = f"gld_vix({vx})"
            skip_groups["gld"].append(f"{sym}({vx})")
        elif data.get("in_cpi_blackout"):
            d_cpi = data.get("days_to_cpi")
            reason = f"cpi_bl({d_cpi}d)"
            skip_groups["cpi"].append(f"{sym}({d_cpi})")
        elif data.get("in_nfp_blackout"):
            d_nfp = data.get("days_to_nfp")
            reason = f"nfp_bl({d_nfp}d)"
            skip_groups["nfp"].append(f"{sym}({d_nfp})")
        elif data.get("in_boj_blackout"):
            d_boj = data.get("days_to_boj")
            reason = f"boj_bl({d_boj}d)"
            skip_groups["boj"].append(f"{sym}({d_boj})")
        elif data.get("in_fomc_blackout"):
            d_fm = data.get("days_to_fomc")
            reason = f"fomc_bl({d_fm}d)"
            skip_groups["fomc"].append(f"{sym}({d_fm})")
        elif data.get("in_earnings_blackout"):
            if data.get("earnings_unknown_risk"):
                bd = cfg.get("earnings_blackout")
                reason = f"earn_unk({bd}d)"
                skip_groups["eunk"].append(f"{sym}({bd})")
            else:
                de = stock.get("days_to_earnings")
                reason = f"earn_bl({de}d)"
                skip_groups["ebl"].append(f"{sym}({de})")
        elif ARGS.block_post_earnings and data.get("post_earnings_vol"):
            de = stock.get("days_to_earnings")
            reason = f"post_earn_bl({de}d)"
            skip_groups["pebl"].append(f"{sym}({de})")
        elif data.get("ivr_meets_threshold") is False:
            ivr_v = stock.get("ivr")
            reason = f"IVR低({ivr_v}%<{cfg.get('ivr_min')}%)"
            skip_groups["ivrl"].append(f"{sym}({ivr_v})")
        elif data.get("ivr_meets_threshold") is None:
            tag = "hv" if "hv_proxy" in str(stock.get("ivr_label", "")) else "no_data"
            reason = f"IVR未知({tag})"
            skip_groups["ivu"].append(f"{sym}({tag})")
        elif not data.get("best_contracts"):
            reason = "无合格合约"
            skip_groups["nc"].append(sym)
        else:
            reason = "未触发"
            skip_groups["nt"].append(sym)

        skip_counts[reason] = skip_counts.get(reason, 0) + 1
        if not ARGS.compact_output:
            dt = _format_skip_drop(sym, data.get("opportunity_alert") or {})
            if dt:
                skip_groups["drop"].append(dt)

    market = raw_scan.get("market", {})
    sp500  = market.get("sp500") or {}
    gates  = raw_scan.get("pre_screen_gates", [])

    # ── skip 输出：按类合并计数，去掉逐天细分的冗长 by_reason ──────────────
    # 类码映射（与 skip_groups 键名一致）
    _CAT_COUNTS: dict[str, int] = {}
    for _sk, _lst in skip_groups.items():
        if _lst:
            _CAT_COUNTS[_sk] = len(_lst)
    # 错误类从 skip_counts 里提取
    _err_total = sum(v for k, v in skip_counts.items() if k.startswith("数据异常"))
    if _err_total:
        _CAT_COUNTS["err"] = _err_total

    _total_skip = sum(skip_counts.values())

    if ARGS.compact_output:
        # compact：只输出总数 + 类码计数（无标的名）
        skip_output: dict = {"count": _total_skip}
        _SKIP_KEY_ORDER = ("nc","ebl","eunk","pebl","ivrl","ivu","gld","fomc","cpi","nfp","boj","nt","err")
        for _sk in _SKIP_KEY_ORDER:
            if _CAT_COUNTS.get(_sk):
                skip_output[_sk] = _CAT_COUNTS[_sk]
    elif ARGS.full_output:
        # full：输出类码计数 + 各类的标的名列表
        skip_output = {"count": _total_skip}
        _SKIP_KEY_ORDER = ("nc","ebl","eunk","pebl","ivrl","ivu","gld","fomc","cpi","nfp","boj","nt","err","drop")
        for _sk in _SKIP_KEY_ORDER:
            if skip_groups.get(_sk):
                skip_output[f"{_sk}_n"] = len(skip_groups[_sk])
                skip_output[_sk] = ",".join(skip_groups[_sk])
    else:
        # 默认：类码计数（无标的名，比 by_reason 紧凑得多）
        skip_output = {"count": _total_skip}
        _SKIP_KEY_ORDER = ("nc","ebl","eunk","pebl","ivrl","ivu","gld","fomc","cpi","nfp","boj","nt","err","drop")
        for _sk in _SKIP_KEY_ORDER:
            if _CAT_COUNTS.get(_sk):
                skip_output[_sk] = _CAT_COUNTS[_sk]

    hv_proxy_count_universe = sum(
        1 for _, d in tickers.items()
        if "hv_proxy" in str((d.get("stock", {}) or {}).get("ivr_label", ""))
    )
    hv_proxy_count_signal = sum(
        1 for s in signals
        if "hv_proxy" in str(s.get("warn") or [])
    )
    total_tickers = len(tickers)
    # hv_proxy 全量时用 all_hv 标记代替两个计数字段（信息量更低时更紧凑）
    all_hv = (hv_proxy_count_universe == total_tickers)

    # all_hv=True 时，每条信号的 ivs 字段与 warn 中的 hv_proxy 已由 meta.all_hv 统一表达，
    # 逐条重复是冗余，此处级联清理以节省 token（116标的/8信号典型场景 ≈ 省 200 字符）
    if all_hv:
        for sig in signals:
            sig.pop("ivs", None)
            w = sig.get("warn")
            if isinstance(w, list):
                w = [x for x in w if x != "hv_proxy"]
                if w:
                    sig["warn"] = w
                else:
                    sig.pop("warn", None)

    gate_score = next((g.get("score") for g in gates if "Gate-5" in str(g.get("name", ""))), None)
    delta_adj = next(
        (g.get("delta_adj", 0.0) for g in gates if "Gate-3" in str(g.get("name", ""))),
        0.0,
    ) or 0.0
    margin_in = ARGS.margin_used
    gate4_ok = any(
        "Gate-4" in str(g.get("name", ""))
        and bool(g.get("passed"))
        and not g.get("skipped", False)
        for g in gates
    )

    meta_out = _drop_none({
            "ts":      raw_scan.get("scan_time"),
            "ver":     __version__,
            "vix":     market.get("vix"),
            "vix3m":   market.get("vix3m"),
            "term":    market.get("term_structure"),            # NORMAL/INVERTED
            "sp_ma200": sp500.get("above_ma200"),
            "sp_dd20": sp500.get("drawdown_20d"),
            "gates_ok": all(bool(g.get("passed")) for g in gates) if gates else None,
            "gate_score": gate_score,
            "fomc_d": market.get("days_to_fomc"),
            "cpi_d":  market.get("days_to_cpi"),
            "nfp_d":  market.get("days_to_nfp"),
            "boj_d":  market.get("days_to_boj"),
            # scope/off_cnt/sug_cnt 固定不变，省略以节省 token
            "margin": {"input": margin_in, "verified": gate4_ok} if margin_in is not None else None,
            "d_adj": delta_adj if delta_adj else None,
            "sig_cnt": f"{len(signals)}/{total_tickers}",
            # hv_proxy：全量时用 all_hv 标记；部分时才输出具体计数
            "all_hv":  True if all_hv else None,
            "hv_proxy_count":     None if all_hv else hv_proxy_count_universe,
            "hv_proxy_sig_count": None if all_hv else hv_proxy_count_signal,
    })

    out = {
        "m": meta_out,
        "sig":  signals,
        "skip": skip_output,
    }
    if ARGS.with_legend:
        meta_key_desc = {
            "all_hv": "true when ALL tickers use HV-proxy IVR (no real IV data available)",
            "hv_proxy_count": "hv-proxy count in universe (only when not all_hv)",
            "hv_proxy_sig_count": "hv-proxy count within signals (only when not all_hv)",
            "gate_score": "Gate-5 three-factor score (0-100, pass>=50)",
            "fomc_d": "days to next FOMC meeting",
            "cpi_d": "days to next US CPI release (prefilled calendar)",
            "nfp_d": "days to next US NFP release (prefilled calendar)",
            "boj_d": "days to next BOJ policy date (prefilled calendar)",
            "margin.input": "user-supplied margin-used%, null if not provided",
            "margin.verified": "true if --margin-used passed and Gate-4 ok",
            "d_adj": "DELTA_MIN tightening when term structure inverted",
            "sp_dd20": "SP500 20-day drawdown%(+ means peak-to-current drawdown, 0 means no drawdown)",
        }
        active_meta_keys = {}
        for mk, desc in meta_key_desc.items():
            top_key = mk.split(".")[0]
            if top_key in out["m"]:
                active_meta_keys[mk] = desc

        out["legend"] = {
            "tkr": "ticker",
            "g": "grade(A+/A/B/C, risk-tier from conservative to aggressive)",
            "off": "official_watchlist_ticker — key present only when true",
            "sec": "sector_bucket(theme filter)",
            "cc": "cc_rule tag(暂停/价差/etc.)",
            "px": "underlying_price",
            "dte": "days_to_expiry",
            "exp": "expiry_date",
            "ivr": "implied_vol_rank_pct",
            "ivs": "ivr_source(real|hv)",
            "ivt": "iv_trend: up=rising,dn=falling,fl=flat — omitted when unknown(see warn:ivt_unknown)",
            "earn": "days_to_earnings(>0:before,<0:after,omitted when>60 or unknown)",
            "earn_note": "post_earnings_Nd when earn<0",
            "th": "thresholds — only in --full-output mode",
            "warn": "warnings(cc:xxx removed; already in cc field)",
            "opp": "opportunity_alert_summary",
            "c": "contracts",
            "meta_keys": active_meta_keys,
            "contract_keys": {
                "k": "strike",
                "del": "delta(2dp)",
                "yld": "annualized_yield_pct",
                "otm": "otm_pct",
                "prem": "mid_premium",
                "spd": "bid_ask_spread_pct",
                "oi": "open_interest",
                "vol": "daily_volume",
                "gs": "greeks_source(r=real,bs=black_scholes)",
                "str": "strategy(csp,csp_spread,bps,fbps,fspread,...)",
            },
            "warn_codes": {
                "earn_bl": "in earnings blackout window",
                "near_blackout": "near earnings blackout window",
                "earn_unk": "earnings date unavailable, treated conservatively",
                "post_earnings_vol": "within post-earnings elevated vol window",
                "post_earn_bl": "blocked by --block-post-earnings flag",
                "fomc_bl": "in FOMC blackout window",
                "cpi_bl": "in CPI release blackout window",
                "nfp_bl": "in NFP release blackout window",
                "boj_bl": "in BOJ policy blackout window",
                "gld_vix_blocked": "GLD blocked by VIX regime rule",
                "hv_proxy": "IVR is proxied by historical volatility",
                "iv_rising": "IV trend is rising",
                "iv_rising_blocked": "signal blocked due to rising IV and strict flag",
                "ivt_unknown": "IV trend unavailable/insufficient data",
            },
            "skip_codes": {
                "count": "total skipped tickers",
                "nc": "no qualifying contracts — count(default) or ticker list(full-output)",
                "ebl": "earnings blackout — count or SYM(days) list",
                "eunk": "earnings unknown risk — count or SYM(blackout_days) list",
                "pebl": "post-earnings vol blocked — count or list",
                "ivrl": "IVR below min — count or SYM(ivr) list",
                "ivu": "IVR missing — count or SYM(hv|no_data) list",
                "gld": "GLD VIX regime — count or SYM(vix) list",
                "fomc": "FOMC blackout — count or SYM(days) list",
                "cpi": "CPI blackout — count or list",
                "nfp": "NFP blackout — count or list",
                "boj": "BOJ blackout — count or list",
                "nt": "no signal trigger — count or list",
                "err": "data error — count or SYM:msg list",
                "drop": "opportunity hint on skip (full-output only)",
            },
        }
    return out


# ═══════════════════════════════════════════════════════════
#  清理旧文件
# ═══════════════════════════════════════════════════════════

def remove_existing_llm_files(directory: Path) -> int:
    """删除目录下符合 LLM_YYYYMMDD_HHMM*.txt 命名（含 _compact/_full）的旧扫描输出。"""
    n = 0
    for f in directory.iterdir():
        if f.is_file() and _LLM_OUTPUT_TXT_RE.match(f.name):
            f.unlink()
            n += 1
    return n


def cleanup_old_files(directory: Path, keep: int = 200):
    raw_files = sorted(
        directory.glob("openclaw_scan_*.json"),
        key=lambda f: f.stat().st_mtime, reverse=True
    )
    llm_files = sorted(
        (f for f in directory.glob("LLM_*.txt") if _LLM_OUTPUT_TXT_RE.match(f.name)),
        key=lambda f: f.stat().st_mtime, reverse=True
    )
    removed_raw = sum(1 for f in raw_files[keep:] if (f.unlink() or True))
    removed_llm = sum(1 for f in llm_files[keep:] if (f.unlink() or True))
    removed = removed_raw + removed_llm
    if removed:
        print(f"  🗑  已清理旧文件 {removed} 个（raw:{removed_raw}, llm:{removed_llm}，各保留 {keep} 个）")


def _llm_json_text(llm_out: dict) -> str:
    """与 LLM_*.txt 文件保持一致的 JSON 序列化。"""
    if ARGS.pretty_json:
        return json.dumps(llm_out, ensure_ascii=False, indent=2, default=str)
    return json.dumps(llm_out, ensure_ascii=False, separators=(",", ":"), default=str)


def _write_scan_index_html(out_dir: Path, llm_out: dict) -> Path:
    """在脚本目录生成 index.html，内容与 LLM 输出一致。"""
    fp = out_dir / "index.html"
    body_json = _llm_json_text(llm_out)
    fp.write_text(
        "<!DOCTYPE html>\n"
        '<html lang="zh-CN">\n<head>\n'
        '<meta charset="utf-8"/>\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1"/>\n'
        f"<title>OpenClaw 扫描结果 {html.escape(__version__)}</title>\n"
        "<style>\n"
        "  body { font-family: ui-sans-serif, system-ui, sans-serif; margin: 1rem; "
        "background: #0f1419; color: #e6edf3; }\n"
        "  h1 { font-size: 1.1rem; font-weight: 600; color: #58a6ff; }\n"
        "  .meta { color: #8b949e; font-size: 0.875rem; margin: 0.5rem 0 1rem; }\n"
        "  pre { white-space: pre-wrap; word-break: break-word; background: #161b22; "
        "padding: 1rem; border-radius: 8px; border: 1px solid #30363d; overflow-x: auto; }\n"
        "</style>\n</head>\n<body>\n"
        f"<h1>OpenClaw 扫描结果（{html.escape(__version__)}）</h1>\n"
        f"<p class=\"meta\">与本轮 LLM_*.txt 内容一致</p>\n"
        f"<pre>{html.escape(body_json)}</pre>\n"
        "</body>\n</html>\n",
        encoding="utf-8",
    )
    return fp


# ═══════════════════════════════════════════════════════════
#  主扫描流程
# ═══════════════════════════════════════════════════════════

def scan_all():
    print("=" * 72)
    print(f"  OpenClaw · Wheel卖Put · 期权链扫描器 {__version__}")
    print(f"  DTE优先：{DTE_PREFERRED_MIN}-{DTE_PREFERRED_MAX}天  "
          f"Delta：{DELTA_MIN}~{DELTA_MAX}")

    src_icons = "  ".join(
        f"{'✅' if s.is_available() else '⬜'}{s.name}" for s in _SOURCES
    )
    print(f"  数据源：{src_icons}  → 主力：{ROUTER.active_name}")

    if MASSIVE_KEYS_LIST:
        print(f"  Massive密钥池：{_MASSIVE_POOL.key_count}个密钥  "
              f"冷却：{_MASSIVE_POOL.min_gap}s/密钥")
    print(f"  并发：{MAX_WORKERS}线程  价差过滤：≤{MAX_SPREAD_PCT}%  OI门槛：≥{MIN_OI}")

    flags = []
    if ARGS.block_hv_proxy:  flags.append("--block-hv-proxy")
    if ARGS.block_rising_iv: flags.append("--block-rising-iv")
    if ARGS.block_post_earnings: flags.append("--block-post-earnings")
    if ARGS.compact_output:  flags.append("--compact-output")
    if ARGS.full_output:     flags.append("--full-output")
    if ARGS.with_legend:     flags.append("--with-legend")
    if ARGS.pretty_json:     flags.append("--pretty-json")
    if ARGS.batch_size:      flags.append(f"--batch-size {ARGS.batch_size}")
    if ARGS.disable_cboe:    flags.append("--disable-cboe")
    if ARGS.cboe_gap != 1.0: flags.append(f"--cboe-gap {ARGS.cboe_gap}")
    if flags:
        print(f"  选项：{' '.join(flags)}")

    print("=" * 72)
    print()
    logger.info(f"===== OpenClaw {__version__} 开始（主力：{ROUTER.active_name}）=====")

    # ── Step 0：全量扫描范围 ───────────────────────────────
    active_tickers = TICKERS
    print(f"📌 扫描范围：full（全量）  |  标的数 {len(active_tickers)}")
    print()

    _iv_history_health_check(len(active_tickers))

    # ── Step 1：市场环境 ──────────────────────────────────
    print("📊 [1/3] 获取市场环境 + Pre-screen Gate...")
    vix_data   = get_vix()
    sp500_data = get_sp500()

    vix    = vix_data.get("vix")
    vix3m  = vix_data.get("vix3m")
    vix_src = vix_data.get("source", "未知")
    print(f"  VIX: {vix} [{vix_src}]  VIX3M: {vix3m}  "
          f"期限结构: {vix_data.get('term_structure', '未知')}")
    if sp500_data:
        ma_tag = "✅ MA200上方" if sp500_data.get("above_ma200") else "⚠ MA200下方"
        print(f"  S&P500: {sp500_data['price']}  {ma_tag}  "
              f"20日回撤: {sp500_data.get('drawdown_20d')}%")
    print()

    gate_passed, delta_adj, gates = run_pre_screen_gate(
        vix_data, sp500_data, ARGS.margin_used
    )

    print("🔐 Pre-screen Gate：")
    for g in gates:
        print(f"  {'✅' if g.passed else '❌'} {g.name}: {g.reason}")
    print()

    if not gate_passed:
        failed = [g.name for g in gates if not g.passed]
        print(f"❌ Gate 失败（{failed}），本轮终止扫描。")
        logger.warning(f"Gate 失败：{failed}")
        return

    if delta_adj:
        print(f"⚠  期限结构倒挂，DELTA_MIN收紧至 {DELTA_MIN + delta_adj:.2f}"
              f"（排除最激进 Delta 端）\n")

    # ── Step 2：并发扫描标的 ─────────────────────────────
    print(f"🔍 [2/3] 并发扫描 {len(active_tickers)} 个标的（{MAX_WORKERS} 线程）...")
    print()

    now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scan_out = {
        "scan_time":   now,
        "scanner_ver": __version__,
        "data_source": ROUTER.active_name,
        "config": {
            "dte_preferred":         f"{DTE_PREFERRED_MIN}-{DTE_PREFERRED_MAX}",
            "dte_fallback":          f"{DTE_FALLBACK_MIN}-{DTE_FALLBACK_MAX}",
            "delta_range":           f"{DELTA_MIN} ~ {DELTA_MAX}",
            "risk_free_rate":        RISK_FREE_RATE,
            "delta_adj_inverted_ts": delta_adj,
            "max_workers":           MAX_WORKERS,
            "max_spread_pct":        MAX_SPREAD_PCT,
            "min_oi":                MIN_OI,
            "margin_used":           ARGS.margin_used,
            "block_hv_proxy":        ARGS.block_hv_proxy,
            "block_rising_iv":       ARGS.block_rising_iv,
            "block_post_earnings":   ARGS.block_post_earnings,
            "scope_mode":            "full",
        },
        "scope": {"mode": "full", "n": len(active_tickers)},
        "market": {
            "vix":            vix,
            "vix3m":          vix3m,
            "vix_source":     vix_src,
            "term_structure": vix_data.get("term_structure"),
            "sp500":          sp500_data,
            "days_to_fomc":   _days_to_next_fomc(),
            "days_to_cpi":    _days_to_next_cpi(),
            "days_to_nfp":    _days_to_next_nfp(),
            "days_to_boj":    _days_to_next_boj(),
        },
        "pre_screen_gates": [
            {
                "name": g.name,
                "passed": g.passed,
                "reason": g.reason,
                "delta_adj": g.delta_adj,
                "score": g.score,
                "skipped": g.skipped,
            }
            for g in gates
        ],
        "tickers": {}
    }

    total = len(active_tickers)
    sigs  = 0
    done  = 0

    ticker_items = list(active_tickers.items())
    batch_size = ARGS.batch_size if ARGS.batch_size and ARGS.batch_size > 0 else len(ticker_items)
    if batch_size < len(ticker_items):
        print(f"  分批扫描：每批 {batch_size} 个标的")

    for start in range(0, len(ticker_items), batch_size):
        batch = ticker_items[start:start + batch_size]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futures = {
                exe.submit(process_ticker, sym, cfg, delta_adj, vix, sp500_data): sym
                for sym, cfg in batch
            }
            for fut in as_completed(futures):
                sym  = futures[fut]
                done += 1
                try:
                    symbol, tr = fut.result()
                except Exception as e:
                    logger.error(f"{sym} 线程异常：{e}", exc_info=True)
                    tr, symbol = {"status": "ERROR", "error": str(e)}, sym

                scan_out["tickers"][symbol] = tr

                grade   = tr.get("config", {}).get("grade", "?")
                status  = tr.get("status", "ERROR")
                has_sig = tr.get("has_signal", False)

                if status == "OK":
                    if has_sig:
                        sigs += 1
                    price   = tr.get("stock", {}).get("price", "N/A")
                    stock   = tr.get("stock", {})
                    ivr_v   = stock.get("ivr")
                    ivr_lbl = stock.get("ivr_label", "")
                    iv_trend_v = stock.get("iv_trend", "?")
                    ivr_str = f"IVR≈{ivr_v}%" if ivr_v else "IVR=N/A"
                    if "hv_proxy" in (ivr_lbl or ""):
                        ivr_str += "⚠"
                    trend_icon = {"rising": "📈", "falling": "📉", "flat": "➡️"}.get(iv_trend_v, "")
                    dte_v = tr.get("dte", "N/A")
                    bcs   = tr.get("best_contracts", [])

                    earn_warn = ""
                    if tr.get("earnings_unknown_risk"):
                        earn_warn = " 🚨财报未知"
                    elif tr.get("in_earnings_blackout"):
                        earn_warn = " 🚫财报禁区"

                    if has_sig:
                        sig_str = "🟢 有信号"
                    else:
                        sig_str = "🔴 无合格合约"

                    bstr = ""
                    if bcs:
                        c    = bcs[0]
                        icon = "📡" if c.get("greeks_source") == "real" else "📐"
                        bstr = (f"最优:{c['strike']}@{c['mid']} "
                                f"Δ{c['delta']} {c['annualized_yield']}% {icon}")
                    off_star = "★" if tr.get("config", {}).get("official") else " "
                    print(f"  [{done:02d}/{total}] {off_star}{symbol:<5} ({grade}) "
                          f"${price} {ivr_str}{trend_icon} DTE={dte_v} "
                          f"{sig_str}{earn_warn}  {bstr}")
                else:
                    print(f"  [{done:02d}/{total}] {symbol:<6} ({grade}) "
                          f"⚠ {tr.get('error','未知错误')}")

    # ── Step 3：保存 ─────────────────────────────────────
    print()
    print("💾 [3/3] 保存结果...")
    now_dt   = datetime.now()
    date_str = now_dt.strftime("%Y%m%d")
    time_str = now_dt.strftime("%H%M")
    ts       = f"{date_str}_{time_str}"

    mode_suffix = "_compact" if ARGS.compact_output else ("_full" if ARGS.full_output else "")
    llm_fn      = f"LLM_{date_str}_{time_str}{mode_suffix}.txt"
    fp_llm      = SCRIPT_DIR / llm_fn
    llm_out     = build_llm_ready_json(scan_out)

    n_llm_old = remove_existing_llm_files(SCRIPT_DIR)
    if n_llm_old:
        print(f"  🗑  已删除历史 LLM 输出 {n_llm_old} 个，仅保留本轮新生成的文件")
    with open(fp_llm, "w", encoding="utf-8") as f:
        if ARGS.pretty_json:
            json.dump(llm_out, f, ensure_ascii=False, indent=2, default=str)
        else:
            json.dump(llm_out, f, ensure_ascii=False, separators=(",", ":"), default=str)
    llm_kb = fp_llm.stat().st_size / 1024

    raw_msg = "（默认关闭）"
    if ARGS.save_raw_json:
        fn = f"openclaw_scan_{ts}.json"
        fp = SCRIPT_DIR / fn
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(scan_out, f, ensure_ascii=False, indent=2, default=str)
        raw_kb  = fp.stat().st_size / 1024
        raw_msg = f"{fp} ({raw_kb:.1f} KB)"

    summary_fp = SCRIPT_DIR / "scan_summary.txt"
    write_summary(scan_out, summary_fp)
    index_fp = _write_scan_index_html(SCRIPT_DIR, llm_out)
    cleanup_old_files(SCRIPT_DIR, keep=KEEP_FILES)

    logger.info(f"JSON已保存：LLM版={llm_fn}({llm_kb:.1f}KB), 原始版={raw_msg}, 信号={sigs}/{total}")

    print()
    print("=" * 72)
    print(f"✅ 扫描完成！（OpenClaw {__version__}）")
    print(f"   LLM JSON：{fp_llm}  ({llm_kb:.1f} KB)")
    print(f"   浏览器汇总页：{index_fp}")
    print(f"   原始JSON：{raw_msg}")
    print(f"   摘要：{summary_fp}")
    print(f"   有信号标的：{sigs} / {total}")
    print()
    print("📊 图标说明：")
    print("   📡 真实Greeks  📐 BS 估算（sigma 已归一化）")
    print("   IVR⚠  HV代理  📈IV上升  📉IV下降  ➡️IV平稳")
    print("   🚫 财报禁区（已知日期）  🚨 财报日期未知（保守处理）")
    print()
    print("📤 下一步：将 LLM JSON 上传 Claude，说「帮我分析这份扫描数据，选标的卖PUT」")
    print()
    print("⚠ 所有信号须在 IBKR 实盘界面最终验证后方可开仓。")
    print("  本内容仅为数据参考，不构成任何投资建议。期权交易具有高风险。")
    print("=" * 72)
    logger.info(f"===== OpenClaw {__version__} 结束 =====")


def main():
    args = parse_args()
    _apply_args(args)
    _init_runtime()
    scan_all()


if __name__ == "__main__":
    main()
