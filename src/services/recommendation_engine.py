import logging
import time
from time import sleep

from ujson import dumps, loads

from src import properties
from src.db.redis_client import RedisClient

properties.setup_logging()


class RecommendationEngine:
    def __init__(self):
        self._redis_client = RedisClient(host=properties.REDIS_HOST, port=properties.REDIS_PORT)
        self._pubsub = self._redis_client.connection.pubsub()
        self._task_id: int = 0
        self._profile_updated: dict = {}

    def __del__(self):
        self._pubsub.close()

    def start(self) -> None:
        """
        Starts infinite loop in which random profiles are retrieved with given frequency.

        :return None
        """
        self._pubsub.subscribe(**{'dummy': self._dummy_handler})
        self._pubsub.run_in_thread(sleep_time=0.005)
        logging.info('Recommendation engine started')
        # Infinite loop
        while True:
            mock_user_id = properties.get_random_user_id()
            profile: dict = self._retrieve_profile(mock_user_id)
            logging.info(profile)
            logging.info('')
            sleep(properties.PROFILE_RETRIEVAL_FREQUENCY)

    @staticmethod
    def _dummy_handler() -> None:
        """
        Just an empty function.

        :return None
        """
        pass

    def _retrieve_profile(self, user_id: int) -> dict:
        """
        Sends task to profile server and retrieves profile from Redis cache.
        Profile is retrieved after timeout or after profile server response.

        :param user_id: userId of profile to be retrieved
        :return: Profile
        """
        # Start to be used in measurement of whole profile retrieval execution time in seconds
        time_measure_start: float = time.time()

        # Submit task
        self._profile_updated[self._task_id] = False
        self._redis_client.connection.publish('ps_comm', dumps({'taskId': self._task_id, 'userId': user_id}))

        # Subscribe for task response
        self._pubsub.psubscribe(**{'task_{}'.format(self._task_id): self._request_handler})

        # Wait for profile update
        time_start: float = time.time()
        while not self._profile_updated[self._task_id] \
                and time.time() - time_start < properties.PROFILE_UPDATE_TIMEOUT:
            sleep(0.01)

        # Cleanup after profile update wait
        del self._profile_updated[self._task_id]
        self._pubsub.punsubscribe('task_{}'.format(self._task_id))
        self._task_id += 1

        # Profile retrieval
        profile: dict = self._redis_client.get_profile(user_id)
        if profile == {}:
            profile = properties.PROFILE_TEMPLATE
        logging.info('Profile {} retrieval took {}s'.format(user_id, time.time() - time_measure_start))
        return profile

    def _request_handler(self, message) -> None:
        """
        Handles task response messages.

        :param message: Redis pubsub message
        :return None
        """
        task_id = loads(message['data'])['taskId']
        if task_id in self._profile_updated:
            self._profile_updated[task_id] = True
            logging.info('Profile ready to be retrieved before timeout')


def _main() -> None:
    """
    Starts recommendation engine.

    :return None
    """
    RecommendationEngine().start()


if __name__ == '__main__':
    _main()
