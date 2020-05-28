import redis
from ujson import loads, dumps

from src.db.db_client import DbClient


class RedisClient(DbClient):

    def __init__(self, host: str = 'localhost', port: int = 6379):
        self.connection: redis.StrictRedis = redis.StrictRedis(host=host, port=port, db=0)

    def __del__(self):
        self.connection.close()

    def profile_exists(self, user_id: int) -> bool:
        return self._row_exists(user_id)

    def get_profile(self, user_id: int) -> dict:
        if not self.profile_exists(user_id):
            return {}
        return self._get_row(user_id)

    def add_profile(self, user_id: int, profile_json: dict) -> None:
        self._add_row(user_id, profile_json)

    def remove_profile(self, user_id: int) -> dict:
        result: dict = self.get_profile(user_id)
        self._delete_row(user_id)
        return result

    def clear_db(self) -> None:
        self.connection.flushdb()

    def _add_row(self, key: int, value: dict) -> None:
        self.connection.set(key, dumps(value))

    def _get_row(self, key: int) -> dict:
        value: str = self.connection.get(key).decode()
        return loads(value)

    def _get_keys(self) -> list:
        return self.connection.keys()

    def _delete_row(self, key: int) -> None:
        self.connection.delete(key)

    def _row_exists(self, key: int) -> bool:
        return self.connection.exists(key)
