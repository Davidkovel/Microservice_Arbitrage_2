from abc import ABC, abstractmethod

from src.parse_json import JsonParse
from src.product_exchanges import MexcAPI, DexApi
from src.product2_arbitrage_manager import ArbitrageManager, PriceFetcher, SpreadCalculator, ArbitrageNotifier
from src.token_manager import TokenManager
from src.proxy_manager import ProxyManager, ProxyConfig, TokensFileConfig, TokensFileManager, KafkaServerConfig, \
    ServerManager, WebServerConfig


class AbstractFactory(ABC):
    @abstractmethod
    async def create_arbitrage_manager(self) -> ArbitrageManager:
        pass


class ArbitrageFactory(AbstractFactory):
    def __init__(self, config, telegram_bot):
        self.config = config
        self.telegram_bot = telegram_bot

    async def create_arbitrage_manager(self) -> ArbitrageManager:
        proxy_config = ProxyConfig(
            url=self.config["proxy"]["url"],
            login=self.config["proxy"]["login"],
            password=self.config["proxy"]["password"]
        )

        tokens_file_config = TokensFileConfig(
            file_path=self.config["tokens_file"]["path"]
        )

        kafka_server_config = KafkaServerConfig(
            bootstrap_servers=self.config["kafka"]["server_host"]
        )

        web_server_config = WebServerConfig(
            web_host=self.config["web_app"]["web_server_host"],
            web_port=self.config["web_app"]["web_port"]
        )

        proxy_manager = ProxyManager(proxy_config)
        tokens_file_manager = TokensFileManager(tokens_file_config)

        parser = JsonParse(tokens_file_manager)
        list_tokens, list_symbols = parser.parse()

        # Создаем API для бирж
        mexc_api = MexcAPI(proxy_manager, list_symbols)
        dex_api = DexApi(proxy_manager)

        await mexc_api.run_websocket()
        # Ззависимости для ArbitrageManager
        price_fetcher = PriceFetcher(mexc_api, dex_api)
        spread_calculator = SpreadCalculator()
        arbitrage_notifier = ArbitrageNotifier(self.telegram_bot, kafka_server_config.bootstrap_servers, web_server_config.web_host,
                                               web_server_config.web_port)
        token_manager = TokenManager(list_tokens)

        return ArbitrageManager(
            price_fetcher=price_fetcher,
            spread_calculator=spread_calculator,
            arbitrage_notifier=arbitrage_notifier,
            token_manager=token_manager,
            mexc_exchange=mexc_api,
            dex_exchange=dex_api,
        )
