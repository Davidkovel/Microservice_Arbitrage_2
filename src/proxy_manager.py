from dataclasses import dataclass
from aiohttp import BasicAuth


@dataclass
class ProxyConfig:
    url: str
    login: str
    password: str


@dataclass
class TokensFileConfig:
    file_path: str


@dataclass
class KafkaServerConfig:
    bootstrap_servers: str


class ProxyManager:
    def __init__(self, proxy_config: ProxyConfig):
        self.proxy_config = proxy_config

    def get_proxy_url(self) -> str:
        return self.proxy_config.url

    def get_proxy_auth(self) -> BasicAuth:
        return BasicAuth(self.proxy_config.login, self.proxy_config.password)


class TokensFileManager:
    def __init__(self, tokens_config: TokensFileConfig):
        self.tokens_config = tokens_config

    def get_tokens_file_path(self) -> str:
        return self.tokens_config.file_path


class ServerManager:
    def __init__(self, server_config: KafkaServerConfig):
        self.server_config = server_config

    def get_server_host(self):
        print('FDKADASD', self.server_config)
        return self.server_config
