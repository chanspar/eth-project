import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, MagicMock, patch

from src.backend.main import consume_whale_alerts, decode_whale_alert


class BackendConsumerTest(unittest.IsolatedAsyncioTestCase):
    def test_decode_whale_alert_adds_value_eth_when_missing(self):
        payload = b'{"hash":"0x1","value":100000000000000000000}'

        decoded = decode_whale_alert(payload)

        self.assertEqual(decoded["hash"], "0x1")
        self.assertEqual(decoded["value_eth"], 100)

    def test_decode_whale_alert_returns_none_on_invalid_json(self):
        payload = b'invalid json'
        decoded = decode_whale_alert(payload)
        self.assertIsNone(decoded)

    async def test_consumer_closes_when_task_is_cancelled(self):
        consumer = Mock()
        to_thread = AsyncMock(side_effect=[asyncio.CancelledError(), None])

        with patch("src.backend.main.asyncio.to_thread", to_thread):
            with self.assertRaises(asyncio.CancelledError):
                await consume_whale_alerts(consumer)

        consumer.subscribe.assert_called_once()
        self.assertEqual(to_thread.await_args_list[-1].args[0], consumer.close)


class DbInitTest(unittest.IsolatedAsyncioTestCase):
    @patch("src.backend.core.db.asyncpg.create_pool", new_callable=AsyncMock)
    async def test_init_db_pool_creates_pool(self, mock_create_pool):
        from src.backend.core.db import init_db_pool
        
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool
        
        pool = await init_db_pool()
        
        self.assertEqual(pool, mock_pool)
        mock_create_pool.assert_called_once()



if __name__ == "__main__":
    unittest.main()

