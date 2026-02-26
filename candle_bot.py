#!/usr/bin/env python3
"""
Moomoo 1-Minute Candle Trading Bot
===================================
Entry logic:
  - Previous 1-min candle GREEN  → BUY  (go long)
  - Previous 1-min candle RED    → SELL (go short)

Exit logic (checked on every completed candle after entry):
  - STOP LOSS : 2 consecutive candles in the losing direction  → close position
  - TAKE PROFIT: once at least one favorable candle has been seen, the first
                 candle that turns unfavorable               → close position

A "favorable" candle for a LONG  is GREEN; for a SHORT it is RED.
A "losing"    candle for a LONG  is RED;   for a SHORT it is GREEN.

Usage:
  python candle_bot.py US.AAPL 10                         # 10 shares, paper trading
  python candle_bot.py US.TSLA 5 --env real --unlock-pwd YOUR_PWD
"""

import argparse
import logging
import threading
import time

import moomoo as ft

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Candle handler ────────────────────────────────────────────────────────────

class _KLineHandler(ft.CurKlineHandlerBase):
    """
    Detects completed 1-minute candles by watching for time_key changes in the
    real-time push stream.  When the timestamp changes, the *previous* candle is
    finished and its OHLC is final.
    """

    def __init__(self, bot: "CandleBot") -> None:
        super().__init__()
        self.bot = bot
        self._last_time_key: str | None = None
        self._last_open: float | None = None
        self._last_close: float | None = None

    def on_recv_rsp(self, rsp_pb) -> tuple:
        ret, data = super().on_recv_rsp(rsp_pb)
        if ret != ft.RET_OK:
            log.warning("KLine push error: %s", data)
            return ret, data

        for _, row in data.iterrows():
            time_key = row["time_key"]
            open_px  = float(row["open"])
            close_px = float(row["close"])

            if self._last_time_key is None:
                # First tick — initialise tracking state
                self._last_time_key = time_key
                self._last_open     = open_px
                self._last_close    = close_px

            elif time_key != self._last_time_key:
                # New candle just opened → previous candle is now closed
                self.bot.on_candle_close(
                    open_price=self._last_open,
                    close_price=self._last_close,
                    candle_time=self._last_time_key,
                )
                self._last_time_key = time_key
                self._last_open     = open_px
                self._last_close    = close_px

            else:
                # Same candle still forming — keep close updated
                self._last_close = close_px

        return ret, data


# ── Bot ───────────────────────────────────────────────────────────────────────

class CandleBot:
    """
    Parameters
    ----------
    stock_code : str
        Moomoo-format code, e.g. ``'US.AAPL'``.
    qty : int
        Number of shares per trade.
    host : str
        OpenD host (default ``'127.0.0.1'``).
    port : int
        OpenD port (default ``11111``).
    trd_env : ft.TrdEnv
        ``TrdEnv.SIMULATE`` for paper trading, ``TrdEnv.REAL`` for live.
    unlock_pwd : str
        Trade unlock password — only required for ``TrdEnv.REAL``.
    """

    def __init__(
        self,
        stock_code: str,
        qty: int,
        host: str = "127.0.0.1",
        port: int = 11111,
        trd_env: ft.TrdEnv = ft.TrdEnv.SIMULATE,
        unlock_pwd: str = "",
    ) -> None:
        self.stock_code = stock_code
        self.qty        = qty
        self.host       = host
        self.port       = port
        self.trd_env    = trd_env
        self.unlock_pwd = unlock_pwd

        # ── Position state ────────────────────────────────────────────────────
        self.position: str | None = None  # 'long', 'short', or None
        self.consecutive_losing: int = 0
        self.had_favorable: bool = False

        # ── API contexts ──────────────────────────────────────────────────────
        self.quote_ctx: ft.OpenQuoteContext | None = None
        self.trd_ctx:   ft.OpenSecTradeContext | None = None

        self._lock = threading.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def connect(self) -> None:
        log.info("Connecting to OpenD at %s:%d …", self.host, self.port)
        self.quote_ctx = ft.OpenQuoteContext(host=self.host, port=self.port)
        self.trd_ctx   = ft.OpenSecTradeContext(
            filter_trdmarket=ft.TrdMarket.US,
            host=self.host,
            port=self.port,
        )
        if self.trd_env == ft.TrdEnv.REAL and self.unlock_pwd:
            ret, data = self.trd_ctx.unlock_trade(self.unlock_pwd)
            if ret != ft.RET_OK:
                raise RuntimeError(f"unlock_trade failed: {data}")
            log.info("Trade account unlocked.")

    def start(self) -> None:
        """Subscribe to 1-minute candles and begin receiving push data."""
        handler = _KLineHandler(self)
        self.quote_ctx.set_handler(handler)
        self.quote_ctx.start()  # enable async push

        ret, data = self.quote_ctx.subscribe(
            [self.stock_code],
            [ft.SubType.K_1M],
            subscribe_push=True,
        )
        if ret != ft.RET_OK:
            raise RuntimeError(f"subscribe failed: {data}")

        log.info(
            "Bot live on %s (qty=%d, env=%s). Waiting for first candle close …",
            self.stock_code, self.qty,
            "PAPER" if self.trd_env == ft.TrdEnv.SIMULATE else "LIVE",
        )

    def disconnect(self) -> None:
        if self.quote_ctx:
            self.quote_ctx.close()
        if self.trd_ctx:
            self.trd_ctx.close()
        log.info("Disconnected.")

    # ── Strategy core ─────────────────────────────────────────────────────────

    def on_candle_close(
        self, open_price: float, close_price: float, candle_time: str
    ) -> None:
        """
        Called by ``_KLineHandler`` each time a 1-minute candle is confirmed
        closed (i.e. a new candle's first tick has arrived).
        """
        is_green = close_price > open_price
        is_red   = close_price < open_price
        color    = "GREEN" if is_green else ("RED" if is_red else "DOJI")

        log.info(
            "Candle closed [%s]  open=%.4f  close=%.4f  (%s)",
            candle_time, open_price, close_price, color,
        )

        with self._lock:
            if self.position is None:
                self._try_enter(is_green, is_red)
            else:
                self._check_exit(is_green, is_red)

    def _try_enter(self, is_green: bool, is_red: bool) -> None:
        if is_green:
            log.info("Entry signal: GREEN candle → going LONG")
            self._place_order(ft.TrdSide.BUY)
        elif is_red:
            log.info("Entry signal: RED candle → going SHORT")
            self._place_order(ft.TrdSide.SELL)
        else:
            log.info("Doji candle — no entry signal.")

    def _check_exit(self, is_green: bool, is_red: bool) -> None:
        """
        Exit rules applied to the candle that just closed while we hold a position.

        Favorable / losing directions:
          LONG  → favorable=green, losing=red
          SHORT → favorable=red,   losing=green
        """
        if self.position == "long":
            favorable = is_green
            losing    = is_red
        else:  # short
            favorable = is_red
            losing    = is_green

        if favorable:
            self.had_favorable = True
            self.consecutive_losing = 0
            log.info(
                "Favorable candle for %s position. "
                "consecutive_losing reset to 0. had_favorable=True.",
                self.position,
            )

        elif losing:
            if self.had_favorable:
                # Price has reversed after a profitable run → take profit now
                log.info(
                    "First unfavorable candle after favorable run → TAKE PROFIT"
                )
                self._close_position("TAKE PROFIT")
            else:
                self.consecutive_losing += 1
                log.info(
                    "Losing candle #%d for %s position.",
                    self.consecutive_losing, self.position,
                )
                if self.consecutive_losing >= 2:
                    log.info("2 consecutive losing candles → STOP LOSS")
                    self._close_position("STOP LOSS")

        else:  # doji while in position
            log.info("Doji candle while in %s position — holding.", self.position)

    # ── Order helpers ─────────────────────────────────────────────────────────

    def _place_order(self, side: ft.TrdSide) -> None:
        direction = "long" if side == ft.TrdSide.BUY else "short"
        ret, data = self.trd_ctx.place_order(
            price=0,
            qty=self.qty,
            code=self.stock_code,
            trd_side=side,
            order_type=ft.OrderType.MARKET,
            trd_env=self.trd_env,
        )
        if ret == ft.RET_OK:
            order_id = data["order_id"].iloc[0]
            log.info(
                "Order filled (%s, qty=%d) — order_id=%s",
                direction, self.qty, order_id,
            )
            self.position          = direction
            self.consecutive_losing = 0
            self.had_favorable      = False
        else:
            log.error("place_order failed: %s", data)

    def _close_position(self, reason: str) -> None:
        qty_to_close = self._get_position_qty()
        if qty_to_close <= 0:
            log.warning(
                "Tried to close %s but held qty is 0 — resetting state.",
                self.position,
            )
            self.position          = None
            self.consecutive_losing = 0
            self.had_favorable      = False
            return

        # Opposite side to flatten the position
        close_side = ft.TrdSide.SELL if self.position == "long" else ft.TrdSide.BUY

        ret, data = self.trd_ctx.place_order(
            price=0,
            qty=qty_to_close,
            code=self.stock_code,
            trd_side=close_side,
            order_type=ft.OrderType.MARKET,
            trd_env=self.trd_env,
        )
        if ret == ft.RET_OK:
            order_id = data["order_id"].iloc[0]
            log.info(
                "[%s] Closed %s position (qty=%d) — order_id=%s",
                reason, self.position, qty_to_close, order_id,
            )
            self.position          = None
            self.consecutive_losing = 0
            self.had_favorable      = False
        else:
            log.error("close_position failed: %s", data)

    def _get_position_qty(self) -> int:
        """
        Query the actual held quantity from the account so the closing order is
        exact.  Falls back to ``self.qty`` if the query fails.
        """
        ret, data = self.trd_ctx.position_list_query(
            code=self.stock_code,
            trd_env=self.trd_env,
        )
        if ret == ft.RET_OK and not data.empty:
            # 'qty' is the total held quantity regardless of position direction
            held = int(data["qty"].iloc[0])
            return abs(held) if held != 0 else self.qty
        log.warning("position_list_query failed or empty — using configured qty.")
        return self.qty


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Moomoo 1-minute candle momentum bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "stock",
        help="Stock code in moomoo format, e.g. US.AAPL",
    )
    parser.add_argument(
        "qty",
        type=int,
        help="Number of shares to trade per entry",
    )
    parser.add_argument(
        "--host", default="127.0.0.1",
        help="OpenD host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", type=int, default=11111,
        help="OpenD port (default: 11111)",
    )
    parser.add_argument(
        "--env", choices=["paper", "real"], default="paper",
        help="Trading environment: paper (default) or real",
    )
    parser.add_argument(
        "--unlock-pwd", default="",
        dest="unlock_pwd",
        help="Trade unlock password (required when --env real)",
    )
    args = parser.parse_args()

    trd_env = ft.TrdEnv.REAL if args.env == "real" else ft.TrdEnv.SIMULATE

    bot = CandleBot(
        stock_code=args.stock,
        qty=args.qty,
        host=args.host,
        port=args.port,
        trd_env=trd_env,
        unlock_pwd=args.unlock_pwd,
    )

    try:
        bot.connect()
        bot.start()
        log.info("Press Ctrl+C to stop the bot.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutdown requested.")
    finally:
        bot.disconnect()


if __name__ == "__main__":
    main()
