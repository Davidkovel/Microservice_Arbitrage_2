import aiohttp
from utils.logger import logger

class RestApiSender:
    def __init__(self, web_server: str, web_port: str):
        """
        Инициализация HTTP-клиента для отправки уведомлений.

        Args:
            web_server: URL сервера, на который отправляются уведомления.
            web_port: Порт сервера.
        """
        self.base_url = f"http://{web_server}:{web_port}"
        self.session = None  # Сессия будет создана при первом запросе

    async def ensure_session(self):
        """Создает сессию, если она еще не создана."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def send_message(self, value: dict):
        """
        Отправка уведомления на сервер.

        Args:
            value: Словарь с данными уведомления.
        """
        url = f"{self.base_url}/notifications/"
        try:
            await self.ensure_session()  # Убедимся, что сессия существует
            async with self.session.post(url, json=value) as response:
                if response.status == 200:
                    logger.info(f"Sent message to {url}")
                else:
                    logger.error(f"Failed to send message: {response.status}, Response: {await response.text()}")
        except Exception as e:
            logger.error(f"Error sending message to {url}: {e}")
            raise
        finally:
            # Закрываем сессию после отправки (опционально)
            if self.session and not self.session.closed:
                await self.session.close()

    async def close(self):
        """Закрывает сессию."""
        if self.session and not self.session.closed:
            await self.session.close()