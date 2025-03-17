import asyncio

from src.factory import AbstractFactory, ArbitrageFactory

from utils import logger
from config import ConfigLoader


async def run_arbitrage(factory: AbstractFactory):
    """
    Run the arbitrage manager.
    """
    product = factory.create_arbitrage_manager()
    await product.run_find_arbitrage()


async def main():
    """
    Run Arbitrage manager concurrently.
    """
    config = ConfigLoader.load_config()
    factory = ArbitrageFactory(config)
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
