import asyncio

from aiogram_bot.bot import TelegramBot
from src.factory import AbstractFactory, ArbitrageFactory

from utils import logger
from config import ConfigLoader


async def run_bot(telegram_bot: TelegramBot):
    """
    Start the Telegram bot.
    """
    await telegram_bot.start()


async def run_arbitrage(factory: AbstractFactory):
    """
    Run the arbitrage manager.
    """
    product = await factory.create_arbitrage_manager()
    await product.run_find_arbitrage()


async def main():
    """
    Run Arbitrage manager concurrently.
    """
    config = ConfigLoader.load_config()

    telegram_bot = TelegramBot(config["telegram_bot"]["token"])
    factory = ArbitrageFactory(config, telegram_bot)
    try:
        await asyncio.gather(
            run_arbitrage(factory),
        )
    finally:
        pass
        # await arbitrage_manager.deconstruct_http_client()


def turn_off_debug():
    logger.remove()


if __name__ == "__main__":
    print("[INFO] Prod started")
    asyncio.run(main())
    # SWFTC
