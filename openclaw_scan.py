#!/usr/bin/env python3
"""
OpenClaw · Wheel卖Put · 期权链扫描器 v2.1
==========================================
v2.1 核心变更（对比 v2.0）：

  [BUG FIX] at_20d_low窗口包含当天问题修复
       ▸ 改为使用过去20个交易日窗口（不含当天）并用严格小于判定
       ▸ Opportunity Alert 条件5（SPX未创20日新低）恢复真实过滤语义

  [FEATURE] IV历史库启动健康检查
       ▸ scan_all 启动时检查 iv_history.db 近30天覆盖率
       ▸ 覆盖不足时明确告警，提示本轮信号可能大量退化为 HV 代理

  [FEATURE] LLM JSON 关键上下文补齐
       ▸ meta 新增 gate_score / fomc_d / margin / d_adj / sp_dd20
       ▸ signal 新增 th 阈值块；contract 新增 oi/vol
       ▸ 新增 earn_note 字段，关闭 legend 也能识别 earn<0 语义

  [FEATURE] 财报后波动阻断开关
       ▸ 新增 --block-post-earnings
       ▸ 启用时财报后 vol crush 窗口可直接阻断信号

  [OUTPUT] hv_proxy统计拆分
       ▸ 区分全量 hv_proxy_count 与信号内 hv_proxy_sig_count
       ▸ 便于判断是数据质量问题还是信号本身问题

  [CONFIG] legend 默认开启
       ▸ 默认输出字段图例，支持 --no-legend 关闭

v2.0 核心变更（对比 v9.0）：

  [FEATURE] Opportunity Alert C1条件重构（跌幅触发替代连跌天数）
       ▸ 旧C1：连续收阴 ≥ 3个交易日（方向有，幅度无，假阳性多）
       ▸ 新C1：5日跌幅 ≥ 分档阈值 OR 3日跌幅 ≥ 分档阈值
       ▸ 分档阈值（按评级）：
           A+/A：5日≥5%  OR  3日≥3%
           B：   5日≥6%  OR  3日≥4%
           C：   5日≥12% OR  3日≥8%
       ▸ 优势：过滤小幅震荡假信号；与IV溢价天然强相关；幅度可见
       ▸ consec_down 保留为辅助参考字段，不再作为触发条件

  [FEATURE] get_stock_data() 新增两个价格跌幅字段
       ▸ drop_5d_pct：(今收 / 5交易日前收 - 1) × 100
       ▸ drop_3d_pct：(今收 / 3交易日前收 - 1) × 100

  [OUTPUT] LLM JSON opp字段扩展
       ▸ 新增 p5d（5日跌幅）、p3d（3日跌幅）、c1_5d、c1_3d 四个字段
       ▸ skip标注升级：⚡drop5d(-6.2%) 替代 ⚡down3d

v9.0 核心变更（对比 v8.0）：

  [FEATURE] Opportunity Alert — 连跌触发层
       ▸ get_stock_data() 新增 consecutive_down_days 字段（连续收阴天数）
       ▸ get_ivr_5d_ago() 新helper：从SQLite IV历史库读取~5交易日前的IVR
       ▸ evaluate_opportunity_alert() 新函数：评估五项触发条件
         条件1：连跌 ≥ 3个交易日
         条件2：IVR较5日前上升 ≥ 15个百分点
         条件3：无财报/FOMC黑名单
         条件4：标的评级 ≥ B（C级不触发）
         条件5：SPX未创近20日新低
       ▸ 全部满足时triggered=True；连跌≥3但其他条件未全满足时partial=True
       ▸ process_ticker() 新增 sp500_data 参数，输出 opportunity_alert 字段
       ▸ get_sp500() 新增 low_20d / at_20d_low 字段（供条件5使用）
       ▸ LLM JSON信号卡新增 opp 字段（triggered/partial/consec_down/ivr_delta_5d）
       ▸ 注意：触发后仍须走完整Pre-screen Gate，仓位上限×50%，尾盘30分钟确认

v8.0 核心变更（对比 v7.0）：

  [BUG FIX #1] BS公式sigma归一化（高危）
       ▸ 原代码将 iv_raw=30.0 直接传入 sigma，等于3000%波动率
       ▸ 修复：iv_for_bs = iv_raw / 100.0 if iv_raw >= 1.0 else iv_raw
       ▸ 影响：无真实Greeks时的Delta估算完全错误 → 已修复

  [BUG FIX #2] Delta调整方向反转（高危）
       ▸ 期限结构倒挂时原代码收紧DELTA_MAX（排除最安全的合约）
       ▸ 修复：倒挂时收紧DELTA_MIN（排除最激进端），保留安全端
       ▸ 效果：倒挂时允许范围从[-0.30,-0.15]收紧为[-0.28,-0.15]

  [BUG FIX #3] 财报黑名单静默失效（高危）
       ▸ yfinance获取失败时 days_to_earnings=None → in_blackout=False
       ▸ 修复：blackout>0 且日期获取失败时，保守默认 in_blackout=True
       ▸ 同时记录 earnings_unknown_risk 警告字段

  [BUG FIX #4] IVR=None信号门控缺失（高危）
       ▸ Python: None is not False == True → IVR未知时信号全通过
       ▸ 修复：has_signal 要求 ivr_meets is True（明确为真才通过）
       ▸ HV代理模式：允许通过但强制标记 warn=["ivr_hv_proxy"]

  [BUG FIX #5] Alpaca Paper零价合约未过滤（中危）
       ▸ bid=0 或 ask=0 的合约会通过价差过滤（spread_pct=None）
       ▸ 修复：bid<=0 or ask<=0 强制跳过

  [FEATURE] IV趋势检测
       ▸ calculate_ivr 新增 iv_trend 字段（rising/flat/falling）
       ▸ IV上升时在 warnings 中标记，不直接屏蔽（保留人工判断空间）
       ▸ 可用 --block-rising-iv 选项开启硬过滤

  [FEATURE] 合约排序优化（OTM安全优先）
       ▸ 原排序：年化收益优先（偏向浅OTM高收益合约）
       ▸ 新排序：OTM深度优先，年化收益次之，流动性最后
       ▸ 符合卖方"先保安全垫，再优化收益"原则

  [CONFIG] 密钥加载（与 v6 对齐，降低「零配置」门槛）
       ▸ .env / 环境变量始终优先于代码内默认值
       ▸ 未设置 MASSIVE_KEYS 时使用与 v6 相同的开发用 Massive 密钥列表
       ▸ 未设置 Alpaca 时使用与 v6 相同的 Paper 默认 key/secret（可被环境变量覆盖）
       ▸ 生产环境请将真实密钥只放在 .env，勿提交仓库

  [PERF] MassiveKeyPool 改进
       ▸ 新增 mark_invalid() 方法，标记401/403失效密钥
       ▸ retry装饰器区分限流错误(429，可重试)与认证错误(401/403，不重试)
       ▸ 等待时间计算移入锁内（消除竞态条件）

  [OUTPUT] LLM精简JSON大幅优化（Token消耗降低~55%）
       ▸ 跳过标的改为聚合字符串（非必要不列明细）
       ▸ 仅输出最优合约（Top 1），次优合约可选
       ▸ 缩短键名（tkr/px/dte/ivr/del/yld/otm/prem）
       ▸ IVR来源：real/hv 取代长字符串
       ▸ 新增 --compact-output 与 --full-output 选项
       ▸ 新增 --block-hv-proxy 选项（拒绝HV代理信号）
       ▸ 新增 --block-rising-iv 选项（拒绝IV上升信号）

─────────────────────────────────────────────────
安装：pip install requests yfinance pandas numpy

配置（推荐脚本同目录 .env；与 v6 一致，缺省使用内置默认密钥与 Massive 地址）：
  MASSIVE_KEYS=key1,key2,...                    （可选，缺省与 v6 相同）
  MASSIVE_BASE_URL=https://api.massivetrader.com/v1  （可选）
  ALPACA_PAPER_KEY / ALPACA_PAPER_SECRET        （可选，缺省与 v6 相同）
  TRADIER_TOKEN=your_tradier_token               （可选）
  POLYGON_API_KEY=your_polygon_key               （可选）

运行示例：
  python openclaw_scan_v7.py
  python openclaw_scan_v7.py --compact-output
  python openclaw_scan_v7.py --block-hv-proxy --block-rising-iv
  python openclaw_scan_v7.py --margin-used 42.5 --save-raw-json

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
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Optional

__version__ = "v2.1"

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

LOG_FILE = Path(__file__).parent / "scanner.log"
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
    # v7.0 新增选项
    parser.add_argument("--compact-output",  action="store_true",
                        help="极简LLM输出（最大限度压缩token，跳过标的仅统计数量）")
    parser.add_argument("--full-output",     action="store_true",
                        help="完整LLM输出（包含所有合约细节，适合调试）")
    parser.add_argument("--block-hv-proxy",  action="store_true",
                        help="v7新增：IVR为HV代理时不产生信号（默认允许但标记警告）")
    parser.add_argument("--block-rising-iv", action="store_true",
                        help="v7新增：IV趋势上升时不产生信号（默认允许但标记警告）")
    parser.add_argument("--block-post-earnings", action="store_true",
                        help="财报后vol crush窗口内不产生信号（默认只warn不block）")
    parser.add_argument("--with-legend", dest="with_legend", action="store_true",
                        help="LLM JSON附带字段图例（默认关闭，可按需开启）")
    parser.add_argument("--no-legend", dest="with_legend", action="store_false",
                        help="关闭LLM JSON字段图例（进一步节省token）")
    parser.add_argument("--pretty-json", action="store_true",
                        help="LLM JSON使用缩进格式输出（默认紧凑以节省token）")
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

# Massive API（.env 优先；无 MASSIVE_KEYS 时与 v6 相同内置列表）
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

# Alpaca Paper API（与 v6 一致：环境变量优先，缺省使用 v6 内置 Paper key/secret）
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

_DEFAULT_TICKERS = {
    "SPY":  {"grade":"A+","ivr_min":25,"ann_min":10,"otm_buffer":0.08,"earnings_blackout":0, "fomc_blackout":5, "structure":"CSP"},
    "QQQ":  {"grade":"A+","ivr_min":28,"ann_min":10,"otm_buffer":0.08,"earnings_blackout":0, "fomc_blackout":5, "structure":"CSP"},
    "EWJ":  {"grade":"A+","ivr_min":30,"ann_min":8, "otm_buffer":0.08,"earnings_blackout":0, "structure":"CSP"},
    "AAPL": {"grade":"A", "ivr_min":38,"ann_min":12,"otm_buffer":0.06,"earnings_blackout":10,"structure":"CSP"},
    "MSFT": {"grade":"A", "ivr_min":35,"ann_min":11,"otm_buffer":0.06,"earnings_blackout":10,"structure":"CSP"},
    "NVO":  {"grade":"A", "ivr_min":40,"ann_min":12,"otm_buffer":0.06,"earnings_blackout":10,"structure":"CSP"},
    "ASML": {"grade":"A", "ivr_min":40,"ann_min":12,"otm_buffer":0.06,"earnings_blackout":10,"structure":"CSP"},
    "MA":   {"grade":"A", "ivr_min":32,"ann_min":10,"otm_buffer":0.06,"earnings_blackout":10,"structure":"CSP"},
    "TSM":  {"grade":"A", "ivr_min":42,"ann_min":13,"otm_buffer":0.06,"earnings_blackout":10,"structure":"CSP"},
    "V":    {"grade":"B", "ivr_min":28,"ann_min":8, "otm_buffer":0.05,"earnings_blackout":10,"structure":"CSP"},
    "GLD":  {"grade":"B", "ivr_min":32,"ann_min":8, "otm_buffer":0.05,"earnings_blackout":0, "special_rules":["gld_vix_gate"], "structure":"CSP"},
    "XLU":  {"grade":"B", "ivr_min":26,"ann_min":7, "otm_buffer":0.05,"earnings_blackout":0, "fomc_blackout":3, "structure":"CSP"},
    "META": {"grade":"C", "ivr_min":60,"ann_min":15,"otm_buffer":0.05,"earnings_blackout":14,"structure":"Bull Put Spread"},
    "NVDA": {"grade":"C", "ivr_min":65,"ann_min":18,"otm_buffer":0.05,"earnings_blackout":14,"structure":"Bull Put Spread"},
    "TSLA": {"grade":"C", "ivr_min":80,"ann_min":20,"otm_buffer":0.05,"earnings_blackout":14,"structure":"Bull Put Spread"},
    "HOOD": {"grade":"C", "ivr_min":65,"ann_min":18,"otm_buffer":0.05,"earnings_blackout":14,"structure":"Bull Put Spread"},
    "IBIT": {"grade":"C", "ivr_min":60,"ann_min":15,"otm_buffer":0.05,"earnings_blackout":0, "structure":"Bull Put Spread"},
    "COIN": {"grade":"C", "ivr_min":75,"ann_min":20,"otm_buffer":0.05,"earnings_blackout":14,"structure":"Bull Put Spread"},
    "MSTR": {"grade":"C", "ivr_min":85,"ann_min":25,"otm_buffer":0.05,"earnings_blackout":14,"structure":"Bull Put Spread"},
    "CRCL": {"grade":"C", "ivr_min":80,"ann_min":20,"otm_buffer":0.05,"earnings_blackout":14,"structure":"Bull Put Spread"},
}

CONFIG_PATH = Path(__file__).parent / "tickers_config.json"
if CONFIG_PATH.exists():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as _f:
            TICKERS = json.load(_f)
        print(f"📂 已加载外部配置：{CONFIG_PATH}（{len(TICKERS)} 个标的）")
    except Exception as _e:
        print(f"⚠ 配置文件加载失败，使用内置默认配置：{_e}")
        TICKERS = _DEFAULT_TICKERS
else:
    TICKERS = _DEFAULT_TICKERS


# ═══════════════════════════════════════════════════════════
#  [v7] 改进的重试装饰器（区分可重试与不可重试错误）
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
#  [v7] 改进的 MassiveKeyPool（失效密钥追踪 + 等待时间竞态修复）
# ═══════════════════════════════════════════════════════════

class MassiveKeyPool:
    """
    线程安全的多密钥轮转池（v7改进）。

    v7 新增：
    - mark_invalid()：将401/403失效密钥标记为无效，从轮转中排除
    - 等待时间计算移入锁内，消除多线程竞态条件
    - 所有有效密钥均失效时抛出明确异常
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

                # [v7] 等待时间在锁内计算（消除竞态）
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
    """
    T0 主力数据源：Massive API
    多密钥轮转；v7改进：401/403时调用 mark_invalid() 排除密钥
    """
    name = "massive"

    def __init__(self, key_pool: MassiveKeyPool, base_url: str, endpoints: dict):
        self._pool      = key_pool
        self._base      = base_url.rstrip("/")
        self._endpoints = endpoints
        self._session   = requests.Session()
        self._session.headers.update({
            "Accept":     "application/json",
            "User-Agent": "OpenClaw/7.0",
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
    OPTIONS_ROUTER 主数据源：Alpaca Paper API + Market Data Snapshots
    v7改进：校验 bid/ask 有效性，过滤零价合约
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
        common = {"Accept": "application/json", "User-Agent": "OpenClaw/7.0"}
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

                # [v7] 零价合约过滤（Paper数据常见问题）
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
OPTIONS_SOURCES = []
OPTIONS_ROUTER = None


def _auto_workers() -> int:
    if ARGS.workers:
        return ARGS.workers
    return {"massive": 5, "tradier": 8, "polygon": 2, "yfinance": 4}.get(ROUTER.active_name, 4)

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
    global _ALPACA_PAPER_SOURCE, OPTIONS_SOURCES, OPTIONS_ROUTER, MAX_WORKERS
    _MASSIVE_POOL = MassiveKeyPool(MASSIVE_KEYS_LIST, min_gap=ARGS.massive_gap)
    _SOURCES = [
        MassiveSource(_MASSIVE_POOL, MASSIVE_BASE_URL, MASSIVE_ENDPOINTS),
        TradierSource(TRADIER_TOKEN, TRADIER_SANDBOX),
        PolygonSource(POLYGON_API_KEY),
        YFinanceSource(),
    ]
    ROUTER = DataRouter(_SOURCES, preferred=ARGS.source)
    _ALPACA_PAPER_SOURCE = AlpacaPaperSource(
        ALPACA_PAPER_KEY, ALPACA_PAPER_SECRET,
        paper_base_url=ALPACA_PAPER_BASE_URL,
        data_base_url=ALPACA_DATA_BASE_URL,
    )
    OPTIONS_SOURCES = [_ALPACA_PAPER_SOURCE] + _SOURCES
    OPTIONS_ROUTER = DataRouter(OPTIONS_SOURCES, preferred="alpaca")
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


def _iv_history_health_check():
    """
    启动自检：统计 IV 历史库近 30 天内有数据的标的数量。
    若覆盖率 < 50%，提示本轮扫描可能大量退化为 HV 代理。
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
        total = len(TICKERS)
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
    [v9] 计算约5个交易日前的IVR（用于Opportunity Alert条件2：IVR涨幅检测）。
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
    in_fomc_blackout: bool,
    grade: str,
    spx_not_new_20d_low: bool,
) -> dict:
    """
    [v10] 评估Opportunity Alert五项触发条件（连跌触发层）。

    五项条件：
      C1  5日跌幅 ≥ 分档阈值 OR 3日跌幅 ≥ 分档阈值（v10重构，替代连跌天数）
          分档阈值：A+/A → 5日≥5% OR 3日≥3%
                   B   → 5日≥6% OR 3日≥4%
                   C   → 5日≥12% OR 3日≥8%
      C2  IVR较5日前上升 ≥ 15个百分点
      C3  无财报 / FOMC黑名单
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

    c3 = not in_blackout and not in_fomc_blackout
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

    # [v7] IV趋势：最近5条 vs 前5条
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
    v7 新增第6个返回值：iv_trend
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

    [v7 Fix] Gate-3 期限结构倒挂调整方向修正：
    原：eff_d_max = DELTA_MAX + delta_adj（-0.15 + (-0.02) = -0.17）
        → 排除最安全的 -0.15/-0.16 合约，方向错误
    修：eff_d_min = DELTA_MIN - delta_adj（-0.30 - (-0.02) = -0.28）
        → 排除最激进的 -0.30/-0.29 合约，缩窄危险端 ✓
    delta_adj 现在的语义：DELTA_MIN 的收紧量（正值表示从危险端收紧）
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

    # Gate-3：VIX期限结构（[v7] 修正delta_adj语义：作用于DELTA_MIN，收紧危险端）
    if ts == "INVERTED":
        delta_adj = 0.02   # [v7] 正值：DELTA_MIN从-0.30收紧至-0.28
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
    [v7 Fix] sigma 必须是小数形式（0.30 = 30%）。
    调用前已在 get_put_chain 中完成归一化。
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

    # [v9] 连跌天数（保留为辅助参考字段）
    closes_list = hist["Close"].tolist()
    consec_down = 0
    for i in range(len(closes_list) - 1, 0, -1):
        if closes_list[i] < closes_list[i - 1]:
            consec_down += 1
        else:
            break
    consec_down = min(consec_down, 10)

    # [v10] 5日/3日跌幅（Opportunity Alert C1触发条件）
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
        "consecutive_down_days": consec_down,    # [v9] 辅助参考
        "drop_5d_pct":          drop_5d_pct,     # [v10] 5日涨跌幅%（负=下跌）
        "drop_3d_pct":          drop_3d_pct,     # [v10] 3日涨跌幅%
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


def _days_to_next_fomc(today=None) -> Optional[int]:
    """返回距离下一次FOMC的天数；若无未来日期则返回None。"""
    base = today or datetime.now().date()
    diffs = []
    for ds in FOMC_DATES:
        try:
            d = datetime.strptime(ds, "%Y-%m-%d").date()
        except Exception:
            continue
        diff = (d - base).days
        if diff >= 0:
            diffs.append(diff)
    return min(diffs) if diffs else None


# ═══════════════════════════════════════════════════════════
#  期权链获取与筛选
# ═══════════════════════════════════════════════════════════

@retry(max_attempts=3, delay=2)
def get_put_chain(ticker: str, expiry: str, price: float,
                  dte: int, otm_buffer: float, atm_iv_ref: list) -> list:
    """
    获取 Put 期权链并完成质量过滤。

    v7 Bug修复：
    #1 bid/ask = 0 → 强制跳过（Paper数据问题）
    #2 BS sigma归一化：iv_raw >= 1.0 时除以100（小数形式传入BS公式）
    """
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

        # [v7 Fix #1] bid/ask 零价过滤
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
            # [v7 Fix #2] BS sigma 归一化（核心Bug修复）
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
    筛选符合 Delta 区间和年化收益要求的最优合约（最多3个）。

    [v7] 排序优先级改为 OTM安全优先：
    原：(volume>0, OI>100, annualized_yield) → 偏向浅OTM高收益（激进）
    新：(otm_pct, annualized_yield, open_interest) → 先保安全垫，再优化收益
    """
    qualified = [
        p for p in puts
        if p.get("delta") is not None
        and d_min <= p["delta"] <= d_max
        and p.get("annualized_yield", 0) >= ann_min
        and p.get("otm_pct", 0) >= otm_buffer_pct * 100
    ]
    # [v7] OTM深度优先，收益次之，流动性最后
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
    delta_adj 语义（v7修正）：
    - delta_adj > 0 时：DELTA_MIN 从 DELTA_MIN 收紧至 DELTA_MIN + delta_adj
    - 即排除最激进（delta绝对值最大）的合约，保留安全端
    - 例：delta_adj=0.02 → DELTA_MIN从-0.30变为-0.28，排除-0.30/-0.29合约

    sp500_data（v9新增）：由scan_all()传入，用于Opportunity Alert条件5（SPX未创20日新低）
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

    # 2. IVR（v7：接收第6个返回值 iv_trend）
    ivr, curr_iv, iv_high, iv_low, ivr_label, iv_trend = calculate_ivr(symbol)

    is_hv_proxy = "hv_proxy" in (ivr_label or "")
    if ARGS.block_hv_proxy and is_hv_proxy:
        ivr_meets = False  # [v7] --block-hv-proxy：HV代理直接不满足
    else:
        ivr_meets = (ivr >= config["ivr_min"]) if ivr is not None else None
        # [v7 Fix #4] None 表示IVR未知（数据不足），不等同于通过
        # ivr_meets=None 时信号不产生（has_signal要求 is True）

    stock.update({
        "ivr": ivr, "ivr_label": ivr_label, "iv_trend": iv_trend,
        "current_iv": curr_iv, "iv_52w_high": iv_high, "iv_52w_low": iv_low,
        "ivr_meets_min": ivr_meets,
    })
    result["stock"] = stock

    # 3. [v7 Fix #3] 财报黑名单保守处理
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

    result["in_earnings_blackout"] = in_blackout
    result["earnings_unknown_risk"] = earnings_unknown_risk
    result["near_earnings_blackout"] = near_blackout
    result["post_earnings_vol"] = post_earnings_vol
    result["in_fomc_blackout"] = in_fomc_blackout
    result["days_to_fomc"] = dte_to_fomc

    # [v9] Opportunity Alert 评估
    ivr_5d_ago = get_ivr_5d_ago(symbol)
    spx_not_new_20d_low = True  # 默认通过（无数据时保守允许）
    if sp500_data:
        spx_not_new_20d_low = not sp500_data.get("at_20d_low", False)
    opp_alert = evaluate_opportunity_alert(
        stock       = stock,
        ivr         = ivr,
        ivr_5d_ago  = ivr_5d_ago,
        in_blackout = in_blackout,
        in_fomc_blackout = in_fomc_blackout,
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

    # [v7 Fix #2] Delta范围：倒挂时收紧 DELTA_MIN（危险端），保留安全端
    eff_d_min = DELTA_MIN + delta_adj   # e.g. -0.30 + 0.02 = -0.28（更严格，排除最激进）
    eff_d_max = DELTA_MAX               # -0.15 不变（安全端保留）

    bcs = find_best_contracts(puts, config["ann_min"], eff_d_min, eff_d_max, config["otm_buffer"])

    special_rules = config.get("special_rules") or []
    gld_vix_blocked = False
    if "gld_vix_gate" in special_rules and vix_val is not None and vix_val < 22:
        gld_vix_blocked = True

    # [v7 Fix #4] has_signal 明确要求 ivr_meets is True
    has_signal = (
        not in_blackout
        and not in_fomc_blackout
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
        f.write(f"  距下次FOMC：{mkt.get('days_to_fomc')} 天\n")

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
        f.write(f"{__version__} 变更说明：\n")
        f.write("  📡=真实Greeks  📐=BS估算（已修复sigma归一化Bug）\n")
        f.write("  real_iv ✓=真实IVR  hv_proxy ⚠=HV代理\n")
        f.write("  IV趋势 📈rising / 📉falling / ➡️flat\n")
        f.write("  财报黑名单：获取失败时保守处理为在黑名单内\n")
        f.write("  IVR=None（数据不足）：不产生信号\n")
        f.write("  Delta倒挂调整：收紧DELTA_MIN（排除最激进端）\n")
        f.write("⚠ 本内容仅为数据参考，不构成任何投资建议。期权交易具有高风险。\n")


# ═══════════════════════════════════════════════════════════
#  [v7] 优化的 LLM 精简输出（Token 消耗降低 ~55%）
# ═══════════════════════════════════════════════════════════

def _compact_opp(opp: Optional[dict]) -> Optional[dict]:
    """[v10] 将opportunity_alert精简为LLM友好的紧凑格式"""
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
    return out


def _drop_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def build_llm_ready_json(raw_scan: dict) -> dict:
    """
    维护约定：
    - 只要本函数输出结构发生变更，必须同步 bump __version__
    - 并重新导出一次 schema 文档（openclaw_schema_vX.Y.md）供下游模型更新

    v7 输出优化策略：
    1. 跳过标的：聚合为简短字符串（非 --full-output 模式）
    2. 默认仅保留最优合约；C级高波动标的保留 Top 2 便于人工比较
       （--full-output 时仍保留 Top 3）
    3. 缩短键名（tkr/px/dte/ivr/del/yld/otm/prem）
    4. IVR来源：real/hv 代替长字符串
    5. 新增 iv_trend、earn_warn 字段
    6. --compact-output：极简模式，跳过标的仅计数
    """
    tickers      = raw_scan.get("tickers", {})
    signals      = []
    skip_counts  = {}   # 跳过原因统计
    skip_details = []   # 跳过详情（非compact模式）

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
                skip_details.append({"t": sym, "r": reason})
            continue

        if has_signal:
            grade = str(cfg.get("grade", "")).upper()
            # 默认模式：C级给2个候选便于人工比较，其余维持1个；full-output 保持原有Top3
            max_contracts = 3 if ARGS.full_output else (2 if grade == "C" else 1)
            bcs       = data.get("best_contracts", [])[:max_contracts]
            ivr_src   = "hv" if "hv_proxy" in str(stock.get("ivr_label", "")) else "real"
            iv_trend  = stock.get("iv_trend", "unknown")

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
            if data.get("gld_vix_blocked"):
                warnings.append("gld_vix_blocked")
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
                    "del":  round(float(c.get("delta", 0)), 3),       # delta
                    "yld":  c.get("annualized_yield"),                 # annualized_yield%
                    "otm":  round(float(c.get("otm_pct", 0)), 2),     # otm%
                    "prem": round(float(c.get("mid", 0)), 2),          # premium
                    "spd":  round(float(c.get("spread_pct", 0)), 1)   # spread%
                    if c.get("spread_pct") is not None else None,
                    "oi":   c.get("open_interest"),
                    "vol":  c.get("volume"),
                    "gs":   "r" if c.get("greeks_source") == "real" else "bs",  # greeks_src
                    "str":  cfg.get("structure", "CSP"),               # strategy structure
                })

            earn_days = stock.get("days_to_earnings")
            earn_note = None
            if isinstance(earn_days, (int, float)) and earn_days < 0:
                earn_note = f"post_earnings_{abs(int(earn_days))}d"

            signal_item = _drop_none({
                "tkr":  sym,
                "g":    cfg.get("grade", "?"),
                "px":   stock.get("price"),
                "dte":  dte,
                "exp":  data.get("expiry"),
                "ivr":  stock.get("ivr"),
                "ivs":  ivr_src,               # iv_source: real/hv
                "ivt":  iv_trend,              # iv_trend
                "earn": earn_days,             # days_to_earnings
                "earn_note": earn_note,        # post_earnings_Nd when earn<0
                "warn": warnings or None,
                "opp":  _compact_opp(data.get("opportunity_alert")),  # [v9] Opportunity Alert
                "th":   {
                    "ivr_min": cfg.get("ivr_min"),
                    "ann_min": cfg.get("ann_min"),
                    "otm_buf": cfg.get("otm_buffer"),
                },
                "c":    contracts_out,
            })
            signals.append(signal_item)
            continue

        # 跳过原因分类
        if data.get("gld_vix_blocked"):
            reason = f"gld_vix({raw_scan.get('market', {}).get('vix')})"
        elif data.get("in_fomc_blackout"):
            reason = f"fomc_bl({data.get('days_to_fomc')}d)"
        elif data.get("in_earnings_blackout"):
            if data.get("earnings_unknown_risk"):
                reason = f"earn_unk({cfg.get('earnings_blackout')}d)"
            else:
                reason = f"earn_bl({stock.get('days_to_earnings')}d)"
        elif ARGS.block_post_earnings and data.get("post_earnings_vol"):
            reason = f"post_earn_bl({stock.get('days_to_earnings')}d)"
        elif data.get("ivr_meets_threshold") is False:
            ivr_v = stock.get("ivr")
            reason = f"IVR低({ivr_v}%<{cfg.get('ivr_min')}%)"
        elif data.get("ivr_meets_threshold") is None:
            reason = f"IVR未知({'hv' if 'hv_proxy' in str(stock.get('ivr_label','')) else 'no_data'})"
        elif not data.get("best_contracts"):
            reason = "无合格合约"
        else:
            reason = "未触发"

        skip_counts[reason] = skip_counts.get(reason, 0) + 1
        if not ARGS.compact_output:
            opp = data.get("opportunity_alert") or {}
            # [v10] 若标的C1触发（价格跌幅达标），即使无信号也标注，供人工关注
            opp_note = ""
            if opp.get("triggered"):
                p5 = opp.get("drop_5d_pct")
                opp_note = f" 🔔OppAlert(5d:{p5}%)"
            elif opp.get("partial"):
                conds = opp.get("conds") or {}
                if conds.get("c1_drop5d"):
                    opp_note = f" ⚡drop5d({opp.get('drop_5d_pct')}%)"
                elif conds.get("c1_drop3d"):
                    opp_note = f" ⚡drop3d({opp.get('drop_3d_pct')}%)"
            skip_details.append({"t": sym, "r": reason + opp_note})

    market = raw_scan.get("market", {})
    sp500  = market.get("sp500") or {}
    gates  = raw_scan.get("pre_screen_gates", [])

    # 跳过信息：compact模式仅统计，否则含简短明细
    if ARGS.compact_output:
        skip_output = {
            "count": sum(skip_counts.values()),
            "by_reason": skip_counts,
        }
    else:
        # 将跳过原因格式化为紧凑字符串（GLM的思路）
        skip_summary_parts = []
        for d in skip_details:
            skip_summary_parts.append(f"{d['t']}({d['r']})")
        skip_output = {
            "count": sum(skip_counts.values()),
            "summary": ", ".join(skip_summary_parts) if skip_summary_parts else "none",
        }

    hv_proxy_count_universe = sum(
        1 for _, d in tickers.items()
        if "hv_proxy" in str((d.get("stock", {}) or {}).get("ivr_label", ""))
    )
    hv_proxy_count_signal = sum(
        1 for s in signals
        if "hv_proxy" in str(s.get("warn") or [])
    )

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
            # meta
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
            "margin": {"input": margin_in, "verified": gate4_ok} if margin_in is not None else None,
            "d_adj": delta_adj if delta_adj else None,
            "sig_cnt": f"{len(signals)}/{len(tickers)}",
            "hv_proxy_count": hv_proxy_count_universe,
            "hv_proxy_sig_count": hv_proxy_count_signal,
    })

    out = {
        "m": meta_out,
        "sig":  signals,
        "skip": skip_output,
    }
    if ARGS.with_legend:
        meta_key_desc = {
            "hv_proxy_count": "count in full ticker universe",
            "hv_proxy_sig_count": "count within current signals only",
            "gate_score": "Gate-5 three-factor score (0-100, pass>=50)",
            "fomc_d": "days to next FOMC meeting",
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
            "px": "underlying_price",
            "dte": "days_to_expiry",
            "exp": "expiry_date",
            "ivr": "implied_vol_rank_pct",
            "ivs": "ivr_source(real|hv)",
            "ivt": "iv_trend(rising|flat|falling|unknown)",
            "earn": "days_to_earnings(>0: before earnings, <0: earnings passed N days ago, null: unknown)",
            "earn_note": "extra earnings context(post_earnings_Nd when earn<0)",
            "th": "thresholds(ivr_min/ann_min/otm_buf for this ticker)",
            "warn": "warnings",
            "opp": "opportunity_alert_summary(t=triggered,p=partial,cd=consecutive_down,p5d/p3d=price_drop_pct,c1_*=trigger_checks,d5=ivr_delta_5d,fail=failed_conditions)",
            "c": "contracts",
            "meta_keys": active_meta_keys,
            "contract_keys": {
                "k": "strike",
                "del": "delta",
                "yld": "annualized_yield_pct",
                "otm": "otm_pct",
                "prem": "mid_premium",
                "spd": "bid_ask_spread_pct",
                "oi": "open_interest",
                "vol": "daily_volume",
                "gs": "greeks_source(r=real,bs=black_scholes)",
                "str": "strategy_structure",
            },
            "warn_codes": {
                "earn_bl": "in earnings blackout window",
                "near_blackout": "near earnings blackout window",
                "earn_unk": "earnings date unavailable, treated conservatively",
                "post_earnings_vol": "within post-earnings elevated vol window",
                "post_earn_bl": "blocked by --block-post-earnings flag",
                "fomc_bl": "in FOMC blackout window",
                "gld_vix_blocked": "GLD blocked by VIX regime rule",
                "hv_proxy": "IVR is proxied by historical volatility",
                "iv_rising": "IV trend is rising",
                "iv_rising_blocked": "signal blocked due to rising IV and strict flag",
                "ivt_unknown": "IV trend unavailable/insufficient data",
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
    if flags:
        print(f"  v8选项：{' '.join(flags)}")

    print("=" * 72)
    print()
    _iv_history_health_check()
    logger.info(f"===== OpenClaw {__version__} 开始（主力：{ROUTER.active_name}）=====")

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
              f"（排除最激进合约，v7修正方向）\n")

    # ── Step 2：并发扫描标的 ─────────────────────────────
    print(f"🔍 [2/3] 并发扫描 {len(TICKERS)} 个标的（{MAX_WORKERS} 线程）...")
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
        },
        "market": {
            "vix":            vix,
            "vix3m":          vix3m,
            "vix_source":     vix_src,
            "term_structure": vix_data.get("term_structure"),
            "sp500":          sp500_data,
            "days_to_fomc":   _days_to_next_fomc(),
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

    total = len(TICKERS)
    sigs  = 0
    done  = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {
            exe.submit(process_ticker, sym, cfg, delta_adj, vix, sp500_data): sym
            for sym, cfg in TICKERS.items()
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
                print(f"  [{done:02d}/{total}] {symbol:<6} ({grade}) "
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
    fp_llm      = Path(llm_fn)
    llm_out     = build_llm_ready_json(scan_out)

    n_llm_old = remove_existing_llm_files(Path("."))
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
        fp = Path(fn)
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(scan_out, f, ensure_ascii=False, indent=2, default=str)
        raw_kb  = fp.stat().st_size / 1024
        raw_msg = f"{fp} ({raw_kb:.1f} KB)"

    summary_fp = Path("scan_summary.txt")
    write_summary(scan_out, summary_fp)
    cleanup_old_files(Path("."), keep=KEEP_FILES)

    logger.info(f"JSON已保存：LLM版={llm_fn}({llm_kb:.1f}KB), 原始版={raw_msg}, 信号={sigs}/{total}")

    print()
    print("=" * 72)
    print(f"✅ 扫描完成！（OpenClaw {__version__}）")
    print(f"   LLM JSON：{fp_llm}  ({llm_kb:.1f} KB)")
    print(f"   原始JSON：{raw_msg}")
    print(f"   摘要：{summary_fp}")
    print(f"   有信号标的：{sigs} / {total}")
    print()
    print("📊 图标说明：")
    print("   📡 真实Greeks  📐 BS估算（v7已修复sigma归一化）")
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
