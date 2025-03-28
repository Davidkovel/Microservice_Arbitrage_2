import asyncio
import json
from venv import logger

import websockets
from typing import Dict, Any, Callable


class MexcWebSocket:
    def __init__(self, callback: Callable[[Dict[str, Any]], None]):
        self.uri = "wss://contract.mexc.com/edge"
        self.websocket = None
        self.subscriptions = set()
        self._running = False
        self.callback = callback

    async def connect(self):
        """Установка соединения с WebSocket"""
        try:
            self.websocket = await websockets.connect(
                self.uri,
                ping_interval=15,
                ping_timeout=10,
                close_timeout=5
            )
            self._running = True
            logger.info("WebSocket connected successfully")
            asyncio.create_task(self._keep_alive())
            asyncio.create_task(self._receive_messages())
        except Exception as e:
            logger.error(f"Connection error: {e}")
            await self._reconnect()

    async def _keep_alive(self):
        """Поддержание соединения"""
        while self._running:
            print('Sending ping...')
            await asyncio.sleep(25)
            await self.send_ping()

    async def send_ping(self):
        """Отправка ping"""
        if self.websocket:
            await self.websocket.send(json.dumps({"method": "ping"}))

    async def _reconnect(self):
        """Переподключение с задержкой"""
        await asyncio.sleep(5)
        logger.info("Attempting to reconnect...")
        await self.connect()

    async def subscribe_ticker(self, symbol: str):
        """Подписка на тикер с проверкой формата"""
        if not symbol.endswith("_USDT"):
            symbol = f"{symbol}_USDT"

        sub_msg = {
            "method": "sub.ticker",
            "param": {"symbol": symbol.upper()}  # Исправленный формат
        }

        try:
            await self.websocket.send(json.dumps(sub_msg))
            self.subscriptions.add(symbol)
            print(f"Subscribed to {symbol}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

    async def _receive_messages(self):
        """Основной цикл приема сообщений"""
        while self._running:
            try:
                message = await self.websocket.recv()
                try:
                    data = json.loads(message)
                    # print(f"Raw message: {data}")  # Для отладки
                    if 'data' in data:
                        self.callback(data)
                except json.JSONDecodeError:
                    logger.error(f"Non-JSON message: {message}")
            except websockets.exceptions.ConnectionClosed:
                logger.error("Connection closed, reconnecting...")
                await self._reconnect()
                break
            except Exception as e:
                logger.error(f"Receive error: {e}")
                await self._reconnect()
                break

    async def disconnect(self):
        """Корректное отключение"""
        self._running = False
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket disconnected")