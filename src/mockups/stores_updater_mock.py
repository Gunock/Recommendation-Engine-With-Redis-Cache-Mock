import logging
import os
import random
from time import sleep

from src import properties
from src.db.cassandra_client import CassandraClient

os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "1"
properties.setup_logging()


class StoresUpdaterMock:
    def __init__(self):
        self._cassandra_client = CassandraClient(host=properties.CASSANDRA_HOST, port=properties.CASSANDRA_PORT)
        self._mock_user_id_range = range(1, properties.PROFILE_COUNT + 1)

    def start(self) -> None:
        """
        Starts infinite loop in which random profiles are updated.

        :return None
        """
        self._initialize_mock_profiles()
        logging.info('Stores updater started')
        # Infinite loop
        while True:
            random_profile: dict = self._generate_random_profile()
            logging.info('Updating profile ' + str(random_profile['userId']))
            self._cassandra_client.add_profile(random_profile['userId'], random_profile)
            sleep(1 / properties.PROFILE_UPDATE_FREQUENCY)

    def _initialize_mock_profiles(self) -> None:
        """
        Saves profiles from mock user id range into Cassandra database.

        :return None
        """
        for user_id in self._mock_user_id_range:
            random_profile: dict = self._generate_random_profile(user_id)
            self._cassandra_client.add_profile(user_id, random_profile)

    def _generate_random_profile(self, user_id: int = None) -> dict:
        """
        Creates profile from template with random values.

        :param user_id: userId to assigned to generated profile
        :return: generated profile
        """
        result: dict = properties.PROFILE_TEMPLATE
        for key in result.keys():
            result[key] = random.random() * 10 - 5

        if user_id is None:
            result['userId'] = random.randint(self._mock_user_id_range.start, len(self._mock_user_id_range))
        else:
            result['userId'] = user_id
        return result


def _main() -> None:
    """
    Starts stores updater mock.

    :return None
    """
    StoresUpdaterMock().start()


if __name__ == "__main__":
    _main()
