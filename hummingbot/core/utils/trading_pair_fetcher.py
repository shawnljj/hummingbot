
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
import importlib
import asyncio
from typing import (
    Dict,
    Any,
    Optional,
    Callable,
    Awaitable,
    List
)
from hummingbot.logger import HummingbotLogger
from hummingbot.client.settings import CONNECTOR_SETTINGS, ConnectorType, ConnectorSetting
import logging
from hummingbot.core.utils.async_utils import safe_ensure_future


class TradingPairFetcher:
    _sf_shared_instance: "TradingPairFetcher" = None
    _tpf_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._tpf_logger is None:
            cls._tpf_logger = logging.getLogger(__name__)
        return cls._tpf_logger

    @classmethod
    def get_instance(cls) -> "TradingPairFetcher":
        if cls._sf_shared_instance is None:
            cls._sf_shared_instance = TradingPairFetcher()
        return cls._sf_shared_instance

    def __init__(self):
        self.trading_pairs: Dict[str, Any] = {}
        self.order_book_snapshots: Dict[str, Any] = {}
        self.order_books: Dict[str, Any] = {}
        self._order_book_snapshot_listener_task: Optional[asyncio.Task] = None
        self._init_order_books_task: Optional[asyncio.Task] = None
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        safe_ensure_future(self.fetch_all())

    async def fetch_all(self):
        for conn_setting in CONNECTOR_SETTINGS.values():
            # print("[SHAWN] --- Conn_setting: ", conn_setting)
            module_name = f"{conn_setting.base_name()}_connector" if conn_setting.type is ConnectorType.Connector \
                else f"{conn_setting.base_name()}_api_order_book_data_source"
            # print("[SHAWN] --- module_name: ", module_name)
            module_path = f"hummingbot.connector.{conn_setting.type.name.lower()}." \
                          f"{conn_setting.base_name()}.{module_name}"
            # print("[SHAWN] --- module_path: ", module_path)
            class_name = "".join([o.capitalize() for o in conn_setting.base_name().split("_")]) + \
                         "APIOrderBookDataSource" if conn_setting.type is not ConnectorType.Connector \
                         else "".join([o.capitalize() for o in conn_setting.base_name().split("_")]) + "Connector"
            # print("[SHAWN] --- class_name: ", class_name)

            module = getattr(importlib.import_module(module_path), class_name)
            # print("[SHAWN] --- module: ", module)

            args = {}
            args = conn_setting.add_domain_parameter(args)

            # print("[SHAWN] --- Module: ",module)
            safe_ensure_future(self.call_fetch_pairs(module.fetch_trading_pairs(**args), conn_setting, module))

    async def call_fetch_pairs(self, fetch_fn: Callable[[], Awaitable[List[str]]], conn_setting: ConnectorSetting, module: str):
        try:
            self.trading_pairs[conn_setting.name] = await fetch_fn
            moduleobj: OrderBookTrackerDataSource = module(self.trading_pairs[conn_setting.name])
            args = {"ev_loop": self._ev_loop, "output": self._order_book_snapshot_stream}
            args = conn_setting.add_domain_parameter(args)

            # print("[SHAWN] --- ITS ALIVE Module: ",module)
            # print("[SHAWN] --- ModuleObj: ",moduleobj._trading_pairs
            safe_ensure_future(self.call_fetch_order_book_snapshots(moduleobj.listen_for_order_book_snapshots(**args), conn_setting.name))
        except Exception:
            self.logger().error(f"Connector {conn_setting.name} failed to retrieve its trading pairs. "
                                f"Trading pairs autocompletion won't work.", exc_info=True)
            # In case of error just assign empty list, this is st. the bot won't stop working
            self.trading_pairs[conn_setting.name] = []

    async def call_fetch_order_book_snapshots(self, fetch_fn: Callable[[], Awaitable[List[str]]], exchange_name: str):
        try:
            self._order_book_snapshot_listener_task = await fetch_fn
            # ob_message: OrderBookMessage = await self._order_book_snapshot_stream.get()
            # self.write_order_book_snapshot_to_db(ob_message, ob_message.trading_pair, exchange_name)
        except Exception:
            self.logger().error(f"Connector {exchange_name} failed to retrieve its order book snapshots. ")
            # In case of error just assign empty list, this is st. the bot won't stop working
            self.order_book_snapshots[exchange_name] = []

    # TODO: ASYNC
    # async def write_order_book_snapshot_to_db(self, obmsg: "OrderBookMessage", tpair: str, exch: str):
    #     session: Session = self.session
    #     for bid in obmsg.bids:
    #         order_book_snapshot: OrderBookSnapshot = OrderBookSnapshot(
    #             trading_pair = tpair,
    #             exchange = exch,
    #             timestamp = obmsg.timestamp,
    #             type = obmsg.type,
    #             price = bid.price,
    #             amount = bid.amount,
    #             update_id = bid.update_id,
    #             is_bid = True,)
    #         session.add(order_book_snapshot)
    #         print("[SHAWN] --- ", order_book_snapshot.__repr__())

    #     for ask in obmsg.asks:
    #         order_book_snapshot: OrderBookSnapshot = OrderBookSnapshot(
    #             trading_pair = tpair,
    #             exchange = exch,
    #             timestamp = obmsg.timestamp,
    #             type = obmsg.type,
    #             price = ask.price,
    #             amount = ask.amount,
    #             update_id = ask.update_id,
    #             is_bid = False)
    #         session.add(order_book_snapshot)
    #         print("[SHAWN] --- ", order_book_snapshot.__repr__())
    #     await session.commit()
