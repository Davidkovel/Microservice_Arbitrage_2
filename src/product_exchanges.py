import time
from abc import ABC, abstractmethod
import asyncio
from collections import deque
import aiohttp

from random import uniform
from aiohttp import BasicAuth
from fake_useragent import UserAgent

from utils.logger import *

# 185.80.149.5:22225:djzbXm91cp:3Va7NTEPoQ
# 37.9.48.123:16648:I45nH9d8sD:MvF7CUQE3G
# 89.19.218.41:33867:ZyGNOY34DF:K6fNZIdOj8
# 45.84.3.155:21981:ghK4XI9duw:D163qeVIsZ
# 45.153.72.144:37262:AWhPuFTU4p:EwRyIUdL3J

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
]


#     {"url": "http://85.195.81.169:11923", "login": "uKaL8Q", "password": "yCvYY8"}
class ExchangeApi(ABC):
    def __init__(self):
        self.session = None

    async def init(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    @abstractmethod
    async def get_account(self):
        pass

    @abstractmethod
    async def get_price_coin(self, coin: str, address_contract: str, chain: str):
        pass


class MexcAPI(ExchangeApi):
    def __init__(self, proxy_manager):
        super().__init__()
        self.base_url = "https://contract.mexc.com/api/v1/contract/fair_price/"
        self.headers = {
            "User-Agent": user_agents[0]
        }
        self.proxy_manager = proxy_manager

    async def get_price_coin(self, coin: str, address_contract=None, chain=None, retries=3) -> dict:
        try:
            symbol = f"{coin}_USDT" if not coin.endswith("_USDT") else coin
            url = f"{self.base_url}{symbol}"

            proxy_url = self.proxy_manager.get_proxy_url()
            proxy_auth = self.proxy_manager.get_proxy_auth()

            async with self.session.get(url, proxy=proxy_url, proxy_auth=proxy_auth, headers=self.headers) as response:
                if response.status == 429:
                    logger.warning(f"Time limit excedeed")
                    await asyncio.sleep(5)

                if response.status != 200:
                    logger.error(f"Mexc HTTP error response status: {response.status}")
                    return {"error": f"HTTP error {response.status}"}

                response_data = await response.json()
                if not response_data.get("success", True):
                    logger.error(f"Mexc error response: {response_data}")
                    await asyncio.sleep(5)
                    return {"error": "Mexc error response"}

                await asyncio.sleep(0.5)
                price = response_data["data"]["fairPrice"]
                # logger.info(f'mexc {coin} {price}')
                return {"price": float(price)}
        except Exception as ex:
            logger.error(f"Mexc exception: {ex}")
            return {"error": str(ex)}

    async def get_account(self):
        return {"account": "account info"}

    async def close_session(self):
        await self.session.close()


# экспозиональна backoff задержка

class DexApi(ExchangeApi):
    def __init__(self, proxy_manager):
        super().__init__()
        self.base_url = "https://api.dexscreener.com/token-pairs/v1/"
        self.headers = {
            "User-Agent": user_agents[0]
        }
        self.proxy_manager = proxy_manager

    async def init(self):
        await super().init()

    async def get_price_coin(self, coin: str, address_contract: str, chain: str) -> dict:
        try:
            url = f"{self.base_url}/{chain}/{address_contract}"

            proxy_url = self.proxy_manager.get_proxy_url()
            proxy_auth = self.proxy_manager.get_proxy_auth()

            async with self.session.get(url, proxy=proxy_url, proxy_auth=proxy_auth, headers=self.headers) as response:
                if response.status == 429:
                    logger.warning(f"Time limit excedeed")
                    await asyncio.sleep(5)
                if response.status != 200:
                    logger.error(f"Dex HTTP error {response.status}")
                    return {"error": "HTTP getting price error"}

                response_data = await response.json()
                for index, data in enumerate(response_data):
                    vol_24 = response_data[index]["volume"]["h24"]
                    vol_6 = response_data[index]["volume"]["h6"]
                    vol_1 = response_data[index]["volume"]["h1"]
                    if vol_24 > 0 and vol_6 > 0 and vol_1 > 0:
                        price_usd = response_data[index]["priceUsd"]

                        # logger.info(f'dex: {coin} - {price_usd}')
                        return {"price": float(price_usd), "vol_24": vol_24, "vol_6": vol_6, "vol_1": vol_1}

                return {"price": 0}

        except Exception as ex:
            logger.error(f"Dex exception: {ex} - {coin}")
            return {"error": str(ex)}

    async def get_account(self):
        return {"account": "account info"}

    async def close_session(self):
        await self.session.close()

# УДАЛИТЬ КЛАСС RATE LIMIT ТАК КАК ОН ОСТАНАВЛИВАЕТ САМ ПРОЦЕСС КОДА!!
# class RateLimiter:


# class RateLimiter:
#     def __init__(self, rate_limit: int, period: float):
#         self.rate_limit = rate_limit
#         self.period = period
#         self.timestamps = deque()
#         self.semaphore = asyncio.Semaphore(rate_limit)
#
#     async def wait_for_capacity(self):
#         now = time.monotonic()
#
#         # Удаляем старые таймстампы
#         while self.timestamps and now - self.timestamps[0] >= self.period:
#             self.timestamps.popleft()
#
#         if len(self.timestamps) >= self.rate_limit:
#             sleep_time = self.period - (now - self.timestamps[0])
#             await asyncio.sleep(sleep_time)
#             return await self.wait_for_capacity()
#
#         return True
#
#     async def acquire(self):
#         await self.semaphore.acquire()
#         now = time.monotonic()
#         self.timestamps.append(now)
#
#     def release(self):
#         self.semaphore.release()
