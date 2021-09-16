#!/usr/bin/env python
import asyncio
from abc import ABC
from collections import deque
from enum import Enum
from hummingbot.model.dm_order_book_snapshot import OrderBookSnapshot

from sqlalchemy.orm.session import Session
import logging
import pandas as pd
import re
from typing import (
    Dict,
    Deque,
    Optional,
    Tuple,
    List)
import time
from hummingbot.core.event.events import OrderBookTradeEvent, TradeType
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.utils.async_utils import safe_ensure_future
from .order_book_message import (
    OrderBookMessageType,
    OrderBookMessage,
)
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.model.sql_connection_manager import SQLConnectionManager

TRADING_PAIR_FILTER = re.compile(r"(BTC|ETH|USDT)$")


class OrderBookTrackerDataSourceType(Enum):
    # LOCAL_CLUSTER = 1 deprecated
    REMOTE_API = 2
    EXCHANGE_API = 3


class OrderBookTracker(ABC):
    PAST_DIFF_WINDOW_SIZE: int = 32
    _obt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._obt_logger is None:
            cls._obt_logger = logging.getLogger(__name__)
        return cls._obt_logger

    def __init__(self, data_source: OrderBookTrackerDataSource, trading_pairs: List[str], domain: Optional[str] = None):
        self._domain: Optional[str] = domain
        self._data_source: OrderBookTrackerDataSource = data_source
        self._trading_pairs: List[str] = trading_pairs
        self._order_books_initialized: asyncio.Event = asyncio.Event()
        self._tracking_tasks: Dict[str, asyncio.Task] = {}
        self._order_books: Dict[str, OrderBook] = {}
        self._tracking_message_queues: Dict[str, asyncio.Queue] = {}
        self._past_diffs_windows: Dict[str, Deque] = {}
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_trade_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()

        self._emit_trade_event_task: Optional[asyncio.Task] = None
        self._init_order_books_task: Optional[asyncio.Task] = None
        self._order_book_diff_listener_task: Optional[asyncio.Task] = None
        self._order_book_trade_listener_task: Optional[asyncio.Task] = None
        self._order_book_snapshot_listener_task: Optional[asyncio.Task] = None
        self._order_book_diff_router_task: Optional[asyncio.Task] = None
        self._order_book_snapshot_router_task: Optional[asyncio.Task] = None
        self._update_last_trade_prices_task: Optional[asyncio.Task] = None

    @property
    def data_source(self) -> OrderBookTrackerDataSource:
        return self._data_source

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_books

    @property
    def ready(self) -> bool:
        return self._order_books_initialized.is_set()

    @property
    def snapshot(self) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
        return {
            trading_pair: order_book.snapshot
            for trading_pair, order_book in self._order_books.items()
        }

    def start(self):
        self.stop()
        self._init_order_books_task = safe_ensure_future(
            self._init_order_books()
        )
        self._emit_trade_event_task = safe_ensure_future(
            self._emit_trade_event_loop()
        )
        self._order_book_diff_listener_task = safe_ensure_future(
            self._data_source.listen_for_order_book_diffs(self._ev_loop, self._order_book_diff_stream)
        )
        self._order_book_trade_listener_task = safe_ensure_future(
            self._data_source.listen_for_trades(self._ev_loop, self._order_book_trade_stream)
        )
        self._order_book_snapshot_listener_task = safe_ensure_future(
            self._data_source.listen_for_order_book_snapshots(self._ev_loop, self._order_book_snapshot_stream)
        )
        self._order_book_diff_router_task = safe_ensure_future(
            self._order_book_diff_router()
        )
        self._order_book_snapshot_router_task = safe_ensure_future(
            self._order_book_snapshot_router()
        )
        self._update_last_trade_prices_task = safe_ensure_future(
            self._update_last_trade_prices_loop()
        )

    def stop(self):
        if self._init_order_books_task is not None:
            self._init_order_books_task.cancel()
            self._init_order_books_task = None
        if self._emit_trade_event_task is not None:
            self._emit_trade_event_task.cancel()
            self._emit_trade_event_task = None
        if self._order_book_diff_listener_task is not None:
            self._order_book_diff_listener_task.cancel()
            self._order_book_diff_listener_task = None
        if self._order_book_snapshot_listener_task is not None:
            self._order_book_snapshot_listener_task.cancel()
            self._order_book_snapshot_listener_task = None
        if self._order_book_trade_listener_task is not None:
            self._order_book_trade_listener_task.cancel()
            self._order_book_trade_listener_task = None

        if self._order_book_diff_router_task is not None:
            self._order_book_diff_router_task.cancel()
            self._order_book_diff_router_task = None
        if self._order_book_snapshot_router_task is not None:
            self._order_book_snapshot_router_task.cancel()
            self._order_book_snapshot_router_task = None
        if self._update_last_trade_prices_task is not None:
            self._update_last_trade_prices_task.cancel()
            self._update_last_trade_prices_task = None
        if len(self._tracking_tasks) > 0:
            for _, task in self._tracking_tasks.items():
                task.cancel()
            self._tracking_tasks.clear()
        self._order_books_initialized.clear()

    async def _update_last_trade_prices_loop(self):
        '''
        Updates last trade price for all order books through REST API, it is to initiate last_trade_price and as
        fall-back mechanism for when the web socket update channel fails.
        '''
        await self._order_books_initialized.wait()
        while True:
            try:
                outdateds = [t_pair for t_pair, o_book in self._order_books.items()
                             if o_book.last_applied_trade < time.perf_counter() - (60. * 3)
                             and o_book.last_trade_price_rest_updated < time.perf_counter() - 5]
                if outdateds:
                    args = {"trading_pairs": outdateds}
                    if self._domain is not None:
                        args["domain"] = self._domain
                    last_prices = await self._data_source.get_last_traded_prices(**args)
                    for trading_pair, last_price in last_prices.items():
                        self._order_books[trading_pair].last_trade_price = last_price
                        self._order_books[trading_pair].last_trade_price_rest_updated = time.perf_counter()
                else:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching last trade price.", exc_info=True)
                await asyncio.sleep(30)

    async def _init_order_books(self):
        """
        Initialize order books
        """
        for index, trading_pair in enumerate(self._trading_pairs):
            self._order_books[trading_pair] = await self._data_source.get_new_order_book(trading_pair)
            self._tracking_message_queues[trading_pair] = asyncio.Queue()
            self._tracking_tasks[trading_pair] = safe_ensure_future(self._track_single_book(trading_pair))
            self.logger().info(f"Initialized order book for {trading_pair}. "
                               f"{index + 1}/{len(self._trading_pairs)} completed.")
            await asyncio.sleep(1)
        self._order_books_initialized.set()

    async def _order_book_diff_router(self):
        """
        Route the real-time order book diff messages to the correct order book.
        """
        last_message_timestamp: float = time.time()
        messages_accepted: int = 0
        messages_rejected: int = 0
        await self._order_books_initialized.wait()
        while True:
            try:
                ob_message: OrderBookMessage = await self._order_book_diff_stream.get()
                trading_pair: str = ob_message.trading_pair

                if trading_pair not in self._tracking_message_queues:
                    messages_rejected += 1
                    continue
                message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
                # Check the order book's initial update ID. If it's larger, don't bother.
                order_book: OrderBook = self._order_books[trading_pair]

                if order_book.snapshot_uid > ob_message.update_id:
                    messages_rejected += 1
                    continue
                await message_queue.put(ob_message)
                messages_accepted += 1

                # Log some statistics.
                now: float = time.time()
                if int(now / 60.0) > int(last_message_timestamp / 60.0):
                    self.logger().debug(f"Diff messages processed: {messages_accepted}, rejected: {messages_rejected}")
                    messages_accepted = 0
                    messages_rejected = 0

                last_message_timestamp = now
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unknown error. Retrying after 5 seconds.", exc_info=True)
                await asyncio.sleep(5.0)

    async def _order_book_snapshot_router(self):
        """
        Route the real-time order book snapshot messages to the correct order book.
        """
        await self._order_books_initialized.wait()
        while True:
            try:
                ob_message: OrderBookMessage = await self._order_book_snapshot_stream.get()
                trading_pair: str = ob_message.trading_pair
                if trading_pair not in self._tracking_message_queues:
                    continue
                message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]

                self.write_order_book_snapshot_to_db(ob_message, trading_pair)
                await message_queue.put(ob_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unknown error. Retrying after 5 seconds.", exc_info=True)
                await asyncio.sleep(5.0)

    # TODO: ASYNC
    def write_order_book_snapshot_to_db(self, obmsg: "OrderBookMessage", tpair: str):
        session: Session = self.session
        for bid in obmsg.bids:
            order_book_snapshot: OrderBookSnapshot = OrderBookSnapshot(
                trading_pair = tpair,
                timestamp = obmsg.timestamp,
                exchange = obmsg.type,
                price = bid.price,
                amount = bid.amount,
                update_id = bid.update_id,
                is_bid = True)
            session.add(order_book_snapshot)
            print("[SHAWN] --- ", order_book_snapshot.__repr__())

        for ask in obmsg.asks:
            order_book_snapshot: OrderBookSnapshot = OrderBookSnapshot(
                trading_pair = tpair,
                timestamp = obmsg.timestamp,
                exchange = obmsg.type,
                price = ask.price,
                amount = ask.amount,
                update_id = ask.update_id,
                is_bid = False)
            session.add(order_book_snapshot)
            print("[SHAWN] --- ", order_book_snapshot.__repr__())
        session.commit()

    @property
    def session(self) -> Session:
        self.trade_fill_db = SQLConnectionManager.get_trade_fills_instance(db_name="dm")
        return self.trade_fill_db.get_shared_session()

    async def _track_single_book(self, trading_pair: str):
        past_diffs_window: Deque[OrderBookMessage] = deque()
        self._past_diffs_windows[trading_pair] = past_diffs_window

        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: OrderBook = self._order_books[trading_pair]
        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0

        while True:
            try:
                message: OrderBookMessage = await message_queue.get()
                if message.type is OrderBookMessageType.DIFF:
                    order_book.apply_diffs(message.bids, message.asks, message.update_id)
                    past_diffs_window.append(message)
                    while len(past_diffs_window) > self.PAST_DIFF_WINDOW_SIZE:
                        past_diffs_window.popleft()
                    diff_messages_accepted += 1

                    # Output some statistics periodically.
                    now: float = time.time()
                    if int(now / 60.0) > int(last_message_timestamp / 60.0):
                        self.logger().debug(f"Processed {diff_messages_accepted} order book diffs for {trading_pair}.")
                        diff_messages_accepted = 0
                    last_message_timestamp = now
                elif message.type is OrderBookMessageType.SNAPSHOT:
                    past_diffs: List[OrderBookMessage] = list(past_diffs_window)
                    order_book.restore_from_snapshot_and_diffs(message, past_diffs)
                    self.logger().debug(f"Processed order book snapshot for {trading_pair}.")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unknown error. Retrying after 5 seconds.", exc_info=True)
                await asyncio.sleep(5.0)

    async def _emit_trade_event_loop(self):
        last_message_timestamp: float = time.time()
        messages_accepted: int = 0
        messages_rejected: int = 0
        await self._order_books_initialized.wait()
        while True:
            try:
                trade_message: OrderBookMessage = await self._order_book_trade_stream.get()
                trading_pair: str = trade_message.trading_pair

                if trading_pair not in self._order_books:
                    messages_rejected += 1
                    continue

                order_book: OrderBook = self._order_books[trading_pair]
                order_book.apply_trade(OrderBookTradeEvent(
                    trading_pair=trade_message.trading_pair,
                    timestamp=trade_message.timestamp,
                    price=float(trade_message.content["price"]),
                    amount=float(trade_message.content["amount"]),
                    type=TradeType.SELL if
                    trade_message.content["trade_type"] == float(TradeType.SELL.value) else TradeType.BUY
                ))

                messages_accepted += 1

                # self._print_all_order_books()

                # Log some statistics.
                now: float = time.time()
                if int(now / 60.0) > int(last_message_timestamp / 60.0):
                    self.logger().debug(f"Trade messages processed: {messages_accepted}, rejected: {messages_rejected}")
                    messages_accepted = 0
                    messages_rejected = 0

                last_message_timestamp = now
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg="Unexpected error routing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)
