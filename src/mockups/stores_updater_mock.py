import os
import random
from time import sleep

from src import properties
from src.db.cassandra_client import CassandraClient

os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "1"

_cassandra_client = CassandraClient()
_mock_user_id_range = range(1, properties.PROFILE_COUNT + 1)
_profile_template = {'genre-Adventure': 0.0, 'genre-Animation': 0.0, 'genre-Children': 0.0, 'genre-Comedy': 0.0,
                     'genre-Fantasy': 0.0, 'genre-Romance': 0.0, 'genre-Drama': 0.0, 'genre-Action': 0.0,
                     'genre-Crime': 0.0, 'genre-Thriller': 0.0, 'genre-Horror': 0.0, 'genre-Mystery': 0.0,
                     'genre-Sci-Fi': 0.0, 'genre-IMAX': 0.0, 'genre-Documentary': 0.0, 'genre-War': 0.0,
                     'genre-Musical': 0.0, 'genre-Film-Noir': 0.0, 'genre-Western': 0.0, 'genre-Short': 0.0}


def start_stores_updater_mock() -> None:
    _initialize_mock_profiles()
    while True:
        random_profile: dict = _generate_random_profile()
        print('updating profile ' + str(random_profile['userId']))
        _cassandra_client.add_profile(random_profile['userId'], random_profile)
        sleep(1 / properties.PROFILE_UPDATE_FREQUENCY)


# Saves profiles from mock user id range into Cassandra database
def _initialize_mock_profiles() -> None:
    for user_id in _mock_user_id_range:
        random_profile: dict = _generate_random_profile(user_id)
        _cassandra_client.add_profile(user_id, random_profile)


def _generate_random_profile(user_id: int = None) -> dict:
    global _profile_template

    result: dict = _profile_template
    for key in result.keys():
        result[key] = random.random() * 10 - 5

    if user_id is None:
        result['userId'] = random.randint(_mock_user_id_range.start, len(_mock_user_id_range))
    else:
        result['userId'] = user_id
    return result


def _main():
    start_stores_updater_mock()


if __name__ == "__main__":
    _main()
