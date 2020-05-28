import os
import multiprocessing
from multiprocessing.pool import ThreadPool
from time import sleep

from ujson import loads, dumps

from src import properties
from src.db.cassandra_client import CassandraClient
from src.db.redis_client import RedisClient

os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "1"

_redis_client = RedisClient()
_cassandra_client = CassandraClient()
_mock_user_id: int = properties.get_random_profile_id()


def start_profile_server() -> None:
    print('starting profile server...')
    _put_all_profiles_in_cache()
    print('profiles loaded to cache')

    # subscribes to profile server communication channel
    pubsub = _redis_client.connection.pubsub()
    pubsub.psubscribe(**{'ps_comm': _request_handler})
    pubsub.run_in_thread(sleep_time=.01)
    print('profile server started')
    while True:
        sleep(10)


def _put_profile_in_cache(user_id: int):
    profile: dict = _cassandra_client.get_profile(user_id)
    _redis_client.add_profile(user_id, profile)


def _put_all_profiles_in_cache():
    for i in range(1 + properties.PROFILE_COUNT + 1):
        _put_profile_in_cache(i)


def _request_handler(message) -> None:
    message_data = loads(message['data'])
    print('received request: ' + str(message_data))
    if 'userId' not in message_data:
        _redis_client.connection.publish('re_comm', dumps({'success': False, 'message': 'incorrect request'}))
        return

    # Creates thread pool and tries to check and update profile in given time
    thread = ThreadPool(processes=1)
    res = thread.apply_async(_update_profile, (int(message_data['userId']),))
    latest: bool = True
    try:
        res.get(timeout=properties.PROFILE_UPDATE_TIMEOUT)
    except multiprocessing.TimeoutError:
        latest = False

    _redis_client.connection.publish('re_comm', dumps({'success': True, 'in_cache': True, 'latest': latest}))
    # If profile not latest in given time, then wait for thread to update it
    if not latest:
        res.get()
        thread.terminate()


def _update_profile(user_id: int):
    if not _redis_client.profile_exists(user_id):
        print('profile {} not present in cache'.format(user_id))
        _put_profile_in_cache(user_id)
    else:
        cass_uuid = _cassandra_client.get_profile_uuid(user_id)
        cache_uuid = _redis_client.get_profile(user_id)['uuid']
        if cache_uuid != cass_uuid:
            print('profile {} in cache is outdated'.format(user_id))
            sleep(properties.get_random_delay_secs())
            _put_profile_in_cache(user_id)
        else:
            print('profile {} in cache is up-to-date'.format(user_id))


def _main() -> None:
    start_profile_server()


if __name__ == "__main__":
    _main()
