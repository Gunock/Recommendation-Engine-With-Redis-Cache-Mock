import logging
import os
from time import sleep

from ujson import loads, dumps

from src import properties
from src.db.cassandra_client import CassandraClient
from src.db.redis_client import RedisClient

os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "1"

properties.setup_logging()


class ProfileServer:
    def __init__(self):
        self._redis_client = RedisClient(host=properties.REDIS_HOST, port=properties.REDIS_PORT)
        self._cassandra_client = CassandraClient(host=properties.CASSANDRA_HOST, port=properties.CASSANDRA_PORT)
        self._mock_user_id: int = properties.get_random_user_id()

    def start(self) -> None:
        """
        Subscribes for pubsub channel for task handling and runs infinite loop.

        :return None
        """
        logging.info('Starting profile server...')
        self._preload_profiles_to_cache()
        logging.info('Profiles loaded to cache')

        # Subscribes to profile server communication channel
        pubsub = self._redis_client.connection.pubsub()
        pubsub.psubscribe(**{'ps_comm': self._request_handler})
        pubsub.run_in_thread(sleep_time=.01)
        logging.info('Profile server started')

        # Infinite loop
        while True:
            # Sleep prevents loop from blocking other threads
            sleep(10)

    def _put_profile_in_cache(self, user_id: int) -> None:
        """
        Retrieves profile from Cassandra database and saves it in Redis cache.

        :param user_id: userId of profile to be saved in cache
        :return None
        """
        profile: dict = self._cassandra_client.get_profile(user_id)
        self._redis_client.add_profile(user_id, profile)

    def _preload_profiles_to_cache(self) -> None:
        """
        Loads profiles into Redis cache with amount based on profile percentage specified in properties.

        :return None
        """
        self._redis_client.clear_db()
        profile_id_list = properties.get_random_user_id_list()
        for i in profile_id_list:
            self._put_profile_in_cache(i)

    def _request_handler(self, message) -> None:
        """
        Handles task messages.

        :param message: Redis pubsub message
        :return: None
        """
        message_data = loads(message['data'])
        task_id = message_data['taskId']
        logging.info('Received request: ' + str(message_data))

        # Sleep with random time represents read delays
        sleep(properties.get_random_delay_secs())
        self._put_profile_in_cache(int(message_data['userId']))
        self._redis_client.connection.publish('task_{}'.format(task_id), dumps({'taskId': task_id, 'success': True}))


def _main() -> None:
    """
    Starts profile server.

    :return None
    """
    ProfileServer().start()


if __name__ == "__main__":
    _main()
