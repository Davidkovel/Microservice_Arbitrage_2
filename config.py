import yaml
import os

from dotenv import load_dotenv

from utils.logger import logger


class ConfigLoader:
    @staticmethod
    def load_config(config_path=None):
        """Load configuration from a YAML file or environment variables"""
        load_dotenv()
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                logger.info(f"Loaded configuration from {config_path}")
        else:
            # Load from environment variables
            config = {
                "proxy": {
                    "url": os.getenv("PROXY_URL"),
                    "login": os.getenv("PROXY_LOGIN"),
                    "password": os.getenv("PROXY_PASSWORD")
                },
                "tokens_file": {
                    "path": os.getenv("TOKENS_FILE_PATH")
                },
                "kafka": {
                    "server_host": "172.19.0.3:9092"
                },
                "web_app": {
                    "web_server_host": "172.17.0.2",
                    "web_port": 80
                },
                "telegram_bot": {
                    "token": os.getenv("TELEGRAM_BOT_TOKEN")
                }
            }
            logger.info("Loaded configuration from environment variables")

        return config
