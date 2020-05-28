import redis
from ujson import loads, dumps

from src import properties
from src.db.db_client import DbClient


class RedisClient(DbClient):

    def __init__(self, host: str = 'localhost', port: int = 6379):
        self.connection: redis.StrictRedis = redis.StrictRedis(host=host, port=port, db=0)

    def __del__(self):
        self.connection.close()

    def profile_exists(self, user_id: int) -> bool:
        """
        Check if profile exists in Redis database.

        :param user_id: userId of profile to be checked
        :return True if profile exists in database, otherwise False
        """
        return self.connection.exists(user_id)

    def get_profile(self, user_id: int) -> dict:
        """
        Retrieves profile from Redis database.

        :param user_id: userId of profile to be retrieved
        :return profile if exists, otherwise empty dict
        """
        if not self.profile_exists(user_id):
            return {}
        return loads(self.connection.get(user_id).decode())

    def add_profile(self, user_id: int, profile_json: dict) -> None:
        """
        Save profile in Redis database.

        :param user_id: userId of profile to be saved
        :param profile_json: profile details
        :return None
        """
        self.connection.set(user_id, dumps(profile_json))
        self.connection.expire(user_id, time=properties.PROFILE_EXPIRE_TIME)

    def remove_profile(self, user_id: int) -> None:
        """
        Removes profile from Redis database.

        :param user_id:
        :return None
        """
        self.connection.delete(user_id)

    def clear_db(self) -> None:
        """
        Removes everything from Redis database.
        """
        self.connection.flushdb()
