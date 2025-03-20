from abc import ABC, abstractmethod
from lib2to3.fixes.fix_input import context
from typing import Dict, Set
import asyncio

from utils.logger import logger


class TokenManager:
    def __init__(self, list_tokens: Dict[str, dict]):
        self.list_tokens = list_tokens
        self.cooldown_tokens: Set[str] = set()
        self.current_tokens_with_spread: Dict[str, float] = {}

    def get_tokens(self) -> Dict[str, dict]:
        """Возвращает токены, которые не находятся в кудоуне."""
        return {k: v for k, v in self.list_tokens.items() if k not in self.cooldown_tokens}

    async def add_to_cooldown(self, token: str, cooldown_time: int):
        """Добавляет токен в cooldown на указанное время."""
        self.cooldown_tokens.add(token)
        await asyncio.sleep(cooldown_time)
        self.cooldown_tokens.remove(token)
        # logger.info(f"[INFO] {token} is back in rotation")

    def update_spread(self, token: str, spread: float):
        """Обновляет спред для токена."""
        self.current_tokens_with_spread[token] = spread

    def remove_spread(self, token: str):
        """Удаляет токен из списка текущих спредов."""
        self.current_tokens_with_spread.pop(token, None)

    def get_current_spread(self, token: str) -> float:
        """Возвращает текущий спред для токена."""
        return self.current_tokens_with_spread.get(token, 0.0)

    def is_token_in_history(self, token: str) -> bool:
        """Проверяет наличие токена в истории."""
        return token in self.current_tokens_with_spread


# Pattern State


class SpreadState(ABC):
    def __init__(self, context):  # context: SpreadContext
        self.context = context

    @abstractmethod
    def handle_spread(self, token: str, spread: float, minimum_spread: float,
                      token_manager: TokenManager) -> bool | dict:
        pass


# 1. проверит есть ли спред больше за minimum_spread или спред больше за 30 тогда мы возращаем True и thread id т.к это новый чат отправим сообщение
# для больше 30 процентов
# 2. если спред есть то проверяем есть ли токен в истории если нет то добавляем в историю и возвращаем True
# 3. если опять спред есть проверяем есть ли токен в списке и тогда переходим следущим состояние SpreadDetectedState чтобы узнать больши ли тогда обновляем спред но главно учесть
# что спред всегда быть должен 4 если да то сообщяем если одинаковый ничево не делаем, если он уменьшился то 4 процентов то удаляем токен из списка
# главно чтобы было в радиусе 4 процентов если больше сообщаем, меньше стал сообщаем, если выровнися удаляем токен из списка


class SpreadContext:
    def __init__(self, token_manager: TokenManager):
        self.token_manager = token_manager
        self._state: SpreadState = HasSpreadState(self)

    def transition_to(self, state: SpreadState):
        self._state = state

    def handle_spread(self, token: str, spread: float, minimum_spread: float) -> dict:
        result = self._state.handle_spread(token, spread, minimum_spread, self.token_manager)
        return result


class HasSpreadState(SpreadState):
    def handle_spread(self, token: str, spread: float, minimum_spread: float,
                      token_manager: TokenManager) -> dict:
        if token_manager.is_token_in_history(token):  # Если токен уже в истории
            self.context.transition_to(CheckSpreadAvailableState(self.context))
            return self.context.handle_spread(token, spread, minimum_spread)
        elif spread >= 30:  # Если спред >= 30%
            token_manager.update_spread(token, spread)
            return {"Has_spread": True, "thread_id": 24}
        elif spread > minimum_spread:  # Если спред > minimum_spread
            token_manager.update_spread(token, spread)
            return {"Has_spread": True, "thread_id": 4294967301}

        # Если спред не превышает minimum_spread
        return {"Has_spread": False, "thread_id": None}


class CheckSpreadAvailableState(SpreadState):
    def handle_spread(self, token: str, spread: float, minimum_spread: float,
                      token_manager: TokenManager) -> dict:
        current_spread = token_manager.get_current_spread(token)

        if spread >= 30:  # Переход в SpreadLifeChangeState только при spread >= 30
            self.context.transition_to(SpreadLifeChangeState(self.context))
            return self.context.handle_spread(token, spread, minimum_spread)
        elif spread < minimum_spread:  # Удаляем токен, если спред меньше minimum_spread
            token_manager.remove_spread(token)
            return {"Has_spread": False, "thread_id": None}
        elif spread > current_spread + 4:  # Спред увеличился на 4% или больше
            token_manager.update_spread(token, spread)
            return {"Has_spread": True, "thread_id": 4294967301}
        elif spread < current_spread - 4:  # Спред уменьшился на 4% или больше
            token_manager.update_spread(token, spread)
            return {"Has_spread": True, "thread_id": 4294967301}

        # Если спред не изменился значительно, обновляем его и возвращаем False
        token_manager.update_spread(token, spread)
        return {"Has_spread": False, "thread_id": None}


class SpreadLifeChangeState(SpreadState):
    def handle_spread(self, token: str, spread: float, minimum_spread: float,
                      token_manager: TokenManager) -> dict:
        current_spread = token_manager.get_current_spread(token)

        if spread < minimum_spread:  # Удаляем токен, если спред меньше minimum_spread
            token_manager.remove_spread(token)
            return {"Has_spread": False, "thread_id": None}
        elif spread >= 30:  # Обрабатываем только спреды >= 30%
            if spread > current_spread + 4:  # Спред увеличился на 4% или больше
                token_manager.update_spread(token, spread)
                print('LIFE CHANGE: Spread increased significantly')
                return {"Has_spread": True, "thread_id": 24}
            elif spread < current_spread - 4:  # Спред уменьшился на 4% или больше
                token_manager.update_spread(token, spread)
                print('LIFE CHANGE: Spread decreased significantly')
                return {"Has_spread": True, "thread_id": 24}

        # Если спред не изменился значительно, обновляем его и возвращаем False
        token_manager.update_spread(token, spread)
        return {"Has_spread": False, "thread_id": None}

# Quai 4 процента, riz 3 процента


