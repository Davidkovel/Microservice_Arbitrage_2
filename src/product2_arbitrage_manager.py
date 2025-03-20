import asyncio
import random

from aiogram_bot.bot import TelegramBot
from src.notifications.rest_api.sender import RestApiSender
from utils import logger
from utils.logger import *
from src.product_exchanges import DexApi, MexcAPI
from src.token_manager import TokenManager, SpreadContext


class PriceFetcher:
    def __init__(self, mexc_api: MexcAPI, dex_api: DexApi):
        self.mexc_api = mexc_api
        self.dex_api = dex_api
        # self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def fetch_prices(self, token: str, address_contract: str, chain: str) -> tuple:
        price_mexc = await self.mexc_api.get_price_coin(token)
        await asyncio.sleep(0.1)
        price_dex = await self.dex_api.get_price_coin(token, address_contract, chain)
        return token, price_mexc, price_dex


class SpreadCalculator:
    @staticmethod
    def calculate_spread(price1: float, price2: float, mexc_higher: bool = True) -> float:
        """
        Вычисляет спред с учетом направления.
        :param price1: Цена на MEXC.
        :param price2: Цена на DEX.
        :param mexc_higher: Если True, спред вычисляется только если price1 > price2.
        :return: Спред в процентах.
        """
        if price1 == 0 or price2 == 0:
            return 0.0
        if mexc_higher and price1 <= price2:  # Проверяем, что цена на MEXC выше, чем на DEX
            return 0.0
        return abs((price1 - price2) / ((price1 + price2) / 2)) * 100


class ArbitrageNotifier:
    def __init__(self, telegram_bot: TelegramBot, bootstrap_servers: str, web_host: str, web_port: str,
                 topic: str = "arbitrage_dex_cex-notifications"):
        self.telegram_bot = telegram_bot
        # self.kafka_producer = KafkaProducer(bootstrap_servers, topic)
        # self.rest_api_producer = RestApiSender(web_host, web_port)

    async def notify(self, token: str, spread: float, price_mexc: float, price_dex: float, contract_address: str,
                     chain: str, thread_id: int):
        dex_url = f"https://dexscreener.com/{chain.lower()}/{contract_address}"
        mexc_url = f"https://futures.mexc.com/exchange?symbol={token}_USDT"

        message = (
            f"*Монета:* `{token}`\n"
            f"*Спред:* `{spread:.2f}%`\n\n"
            f"*MEXC Цена:* `{price_mexc}$`\n\n"
            f"*DEX Цена:* `{price_dex}$`\n"
            f"*Контракт:* `{contract_address}`\n"
            f"*Сеть:* `{chain}`\n\n"
            f"[🔗 Перейти на DEX]({dex_url}) | [🔗 Перейти на MEXC]({mexc_url})\n\n"
            f"🖋️ Created by [XGenius PRO]\n"
        )

        await self.telegram_bot.send_message(message, thread_id, dex_url, mexc_url)

        # For Rest Api Message:
        # rest_api_message = {
        #     "formatted_message": message,
        #     "thread_id": thread_id,
        #     "dex_url": dex_url,  # dex_url на верхнем уровне
        #     "mexc_url": mexc_url  # mexc_url на верхнем уровне
        # }
        #
        # try:
        #     await self.rest_api_producer.send_message(rest_api_message)
        #     logger.info(f"Sent arbitrage message via REST API: {rest_api_message}")
        # except Exception as e:
        #     logger.error(f"Failed to send arbitrage message via REST API: {e}")
        #     raise

        # For Kafka Message:
        # kafka_message = {
        #     "token": token,
        #     "spread": spread,
        #     "price_mexc": price_mexc,
        #     "price_dex": price_dex,
        #     "contract_address": contract_address,
        #     "chain": chain,
        #     "thread_id": thread_id,
        #     "formatted_message": message,
        #     "urls": {
        #         "dex": dex_url,
        #         "mexc": mexc_url
        #     }
        # }
        # logger.info(f"Sending arbitrage message to Kafka Consumer (microservice telegram bot): {kafka_message}")
        # await self.kafka_producer.send_message(token, kafka_message)
        # await self.send_telegram_message(message, message_thread_id=thread_id, dex_url=dex_url, mexc_url=mexc_url)

    def close(self):
        """Flush and clean up resources"""
        self.kafka_producer.flush()


class ArbitrageManager:
    def __init__(
            self,
            price_fetcher: PriceFetcher,
            spread_calculator: SpreadCalculator,
            arbitrage_notifier: ArbitrageNotifier,
            token_manager: TokenManager,
            mexc_exchange: MexcAPI,
            dex_exchange: DexApi
    ):
        self.price_fetcher = price_fetcher
        self.spread_calculator = spread_calculator
        self.arbitrage_notifier = arbitrage_notifier
        self.token_manager = token_manager
        self.spread_context = SpreadContext(token_manager)
        self.mexcExchange = mexc_exchange
        self.dexExchange = dex_exchange

    async def init_http_client(self):
        await self.mexcExchange.init()
        await self.dexExchange.init()

    async def process_token(self, token_info):
        try:
            token, contract_address, chain = token_info["token"], token_info["address_contract"], token_info["chain"]
            result = await self.price_fetcher.fetch_prices(token, contract_address, chain)
            token, price_mexc, price_dex = result

            # logger.info(f'CHECKING {token}, {price_mexc} - {price_dex} ')
            if "error" in price_dex or "error" in price_mexc:
                error_dex_time_limit = price_dex["error"].split("!")[0]
                check_error_dex_time_limit = price_dex["message"]
                if error_dex_time_limit == "Rate limit exceeded" or check_error_dex_time_limit == "Request frequently too fast!":
                    time = random.randint(2, 3)
                    await asyncio.sleep(time)
                logger.error(f"[ERROR] Ошибка получения цен: {token} MEXC: {price_mexc}, DEX: {price_dex}")
                return

            spread = self.spread_calculator.calculate_spread(price_mexc["price"], price_dex["price"], mexc_higher=True)

            minimum_spread = token_info.get('minimum_spread', 6.0)

            result_spread = self.spread_context.handle_spread(token, spread, minimum_spread)
            has_spread, thread_id = result_spread['Has_spread'], result_spread['thread_id']
            if has_spread:
                await self.arbitrage_notifier.notify(token, spread, price_mexc["price"], price_dex["price"],
                                                     contract_address, chain, thread_id)
                logger.info(f"[INFO] Sleeping for 1 minute for {token} to avoid spam...")

                # asyncio.create_task(self.token_manager.add_to_cooldown(token, 20))
        except Exception as ex:
            logger.error(
                f"Failed to fetch prices for {token_info['token']}: {ex}, info mexc {price_mexc}, info dex {price_dex}")

    async def worker(self, queue):
        while True:
            token_info = await queue.get()  # Получаем задачу из очереди
            try:
                await self.process_token(token_info)
            finally:
                queue.task_done()  # Помечаем задачу как выполненную

    async def run_find_arbitrage(self):
        await self.init_http_client()
        queue = asyncio.Queue()  # Создаем очередь задач

        # Создаем и запускаем воркеры
        workers = [asyncio.create_task(self.worker(queue)) for _ in range(10)]

        while True:
            tokens = self.token_manager.get_tokens()

            # Добавляем задачи в очередь
            for token, details in tokens.items():
                token_info = {
                    "token": token,
                    "address_contract": details['contract_address'],
                    "chain": details['chain'],
                    "minimum_spread": details.get('minimum_spread', 6.0)
                }
                await queue.put(token_info)  # Добавляем токен в очередь

            # Ждем, пока все задачи в очереди будут выполнены
            await queue.join()

            logger.info('Sleeping for 30 seconds before the next iteration...')
            await asyncio.sleep(20)
            # time.sleep(10)
