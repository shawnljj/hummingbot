import asyncio
import json
import unittest
from typing import Dict, Awaitable
from unittest.mock import patch, AsyncMock

from hummingbot.connector.exchange.gate_io.gate_io_api_user_stream_data_source import GateIoAPIUserStreamDataSource
from hummingbot.connector.exchange.gate_io.gate_io_auth import GateIoAuth
from test.hummingbot.connector.network_mocking_assistant import NetworkMockingAssistant


class TestGateIoAPIOrderBookDataSource(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"

    def setUp(self) -> None:
        super().setUp()
        self.mocking_assistant = NetworkMockingAssistant()
        gate_io_auth = GateIoAuth(api_key="someKey", secret_key="someSecret")
        self.data_source = GateIoAPIUserStreamDataSource(gate_io_auth, trading_pairs=[self.trading_pair])

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def get_user_trades_mock(self) -> Dict:
        user_trades = {
            "time": 1605176741,
            "channel": "spot.usertrades",
            "event": "update",
            "result": [
                {
                    "id": 5736713,
                    "user_id": 1000001,
                    "order_id": "30784428",
                    "currency_pair": f"{self.base_asset}_{self.quote_asset}",
                    "create_time": 1605176741,
                    "create_time_ms": "1605176741123.456",
                    "side": "sell",
                    "amount": "1.00000000",
                    "role": "taker",
                    "price": "10000.00000000",
                    "fee": "0.00200000000000",
                    "point_fee": "0",
                    "gt_fee": "0",
                    "text": "apiv4"
                }
            ]
        }
        return user_trades

    def get_user_orders_mock(self) -> Dict:
        user_orders = {
            "time": 1605175506,
            "channel": "spot.orders",
            "event": "update",
            "result": [
                {
                    "id": "30784435",
                    "user": 123456,
                    "text": "t-abc",
                    "create_time": "1605175506",
                    "create_time_ms": "1605175506123",
                    "update_time": "1605175506",
                    "update_time_ms": "1605175506123",
                    "event": "put",
                    "currency_pair": f"{self.base_asset}_{self.quote_asset}",
                    "type": "limit",
                    "account": "spot",
                    "side": "sell",
                    "amount": "1",
                    "price": "10001",
                    "time_in_force": "gtc",
                    "left": "1",
                    "filled_total": "0",
                    "fee": "0",
                    "fee_currency": "USDT",
                    "point_fee": "0",
                    "gt_fee": "0",
                    "gt_discount": True,
                    "rebated_fee": "0",
                    "rebated_fee_currency": "USDT"
                }
            ]
        }
        return user_orders

    def get_user_balance_mock(self) -> Dict:
        user_balance = {
            "time": 1605248616,
            "channel": "spot.balances",
            "event": "update",
            "result": [
                {
                    "timestamp": "1605248616",
                    "timestamp_ms": "1605248616123",
                    "user": "1000001",
                    "currency": self.base_asset,
                    "change": "100",
                    "total": "1032951.325075926",
                    "available": "1022943.325075926"
                }
            ]
        }
        return user_balance

    @patch("websockets.connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_user_trades(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        output_queue = asyncio.Queue()
        self.ev_loop.create_task(self.data_source.listen_for_user_stream(self.ev_loop, output_queue))

        resp = self.get_user_trades_mock()
        self.mocking_assistant.add_websocket_text_message(
            websocket_mock=ws_connect_mock.return_value, message=json.dumps(resp)
        )
        ret = self.async_run_with_timeout(coroutine=output_queue.get())

        self.assertEqual(ret, resp)

        resp = self.get_user_orders_mock()
        self.mocking_assistant.add_websocket_text_message(
            websocket_mock=ws_connect_mock.return_value, message=json.dumps(resp)
        )
        ret = self.async_run_with_timeout(coroutine=output_queue.get())

        self.assertEqual(ret, resp)

        resp = self.get_user_balance_mock()
        self.mocking_assistant.add_websocket_text_message(
            websocket_mock=ws_connect_mock.return_value, message=json.dumps(resp)
        )
        ret = self.async_run_with_timeout(coroutine=output_queue.get())

        self.assertEqual(ret, resp)
