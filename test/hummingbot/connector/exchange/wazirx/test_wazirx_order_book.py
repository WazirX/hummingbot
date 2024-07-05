from unittest import TestCase

from hummingbot.connector.exchange.wazirx.wazirx_order_book import WazirxOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class WazirxOrderBookTests(TestCase):

    def test_snapshot_message_from_exchange(self):
        snapshot_message = WazirxOrderBook.snapshot_message_from_exchange(
            msg={
                "timestamp": 1588831243,
                "bids": [
                    ["9253.0", "1.0456"]
                ],
                "asks": [
                    ["9291.0", "0.0119"]
                ]
            },
            timestamp=1640000000.0,
            metadata={"trading_pair": "COINALPHA-HBOT"}
        )

        self.assertEqual("COINALPHA-HBOT", snapshot_message.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_message.type)
        self.assertEqual(1640000000.0, snapshot_message.timestamp)
        self.assertEqual(1588831243, snapshot_message.update_id)
        self.assertEqual(-1, snapshot_message.trade_id)
        self.assertEqual(1, len(snapshot_message.bids))
        self.assertEqual(9253.0, snapshot_message.bids[0].price)
        self.assertEqual(1.0456, snapshot_message.bids[0].amount)
        self.assertEqual(1588831243, snapshot_message.bids[0].update_id)
        self.assertEqual(1, len(snapshot_message.asks))
        self.assertEqual(9291.0, snapshot_message.asks[0].price)
        self.assertEqual(0.0119, snapshot_message.asks[0].amount)
        self.assertEqual(1588831243, snapshot_message.asks[0].update_id)

    def test_diff_message_from_exchange(self):
        diff_msg = WazirxOrderBook.diff_message_from_exchange(
            msg={
                "data":
                {
                    "E": 1631682370000,
                    "a": [
                        [
                            "10.0",
                            "75.0"
                        ]
                    ],
                    "b": [
                        [
                            "6.0",
                            "50.0"
                        ]
                    ],
                    "s": "COINALPHA-HBOT",
                },
                "stream": "COINALPHA-HBOT@depth"
            },
            timestamp=1640000000.0,
            metadata={"trading_pair": "COINALPHA-HBOT"}
        )

        self.assertEqual("COINALPHA-HBOT", diff_msg.trading_pair)
        self.assertEqual(OrderBookMessageType.DIFF, diff_msg.type)
        self.assertEqual(1640000000.0, diff_msg.timestamp)
        self.assertEqual(1631682370000, diff_msg.update_id)
        self.assertEqual(1631682370000, diff_msg.first_update_id)
        self.assertEqual(-1, diff_msg.trade_id)
        self.assertEqual(1, len(diff_msg.bids))
        self.assertEqual(6.0, diff_msg.bids[0].price)
        self.assertEqual(50.0, diff_msg.bids[0].amount)
        self.assertEqual(1631682370000, diff_msg.bids[0].update_id)
        self.assertEqual(1, len(diff_msg.asks))
        self.assertEqual(10.0, diff_msg.asks[0].price)
        self.assertEqual(75.0, diff_msg.asks[0].amount)
        self.assertEqual(1631682370000, diff_msg.asks[0].update_id)

    def test_trade_message_from_exchange(self):
        trade_update = {
            "data":
            {
                "trades":
                [
                    {
                        "E": 1631681323000,
                        "S": "buy",
                        "a": 26946138,
                        "b": 26946169,
                        "m": True,
                        "p": "7.0",
                        "q": "15.0",
                        "s": "COINALPHA-HBOT",
                        "t": 17376030
                    }
                ]
            },
            "stream": "COINALPHA-HBOT@trades"
        }

        trade_message = WazirxOrderBook.trade_message_from_exchange(
            msg=trade_update,
            metadata={"trading_pair": "COINALPHA-HBOT"}
        )

        self.assertEqual("COINALPHA-HBOT", trade_message.trading_pair)
        self.assertEqual(OrderBookMessageType.TRADE, trade_message.type)
        self.assertEqual(1631681323.0, trade_message.timestamp)
        self.assertEqual(-1, trade_message.update_id)
        self.assertEqual(-1, trade_message.first_update_id)
        self.assertEqual(17376030, trade_message.trade_id)
