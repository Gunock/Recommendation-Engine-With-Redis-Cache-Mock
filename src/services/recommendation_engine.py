from time import sleep

from ujson import loads, dumps

from src import properties
from src.db.redis_client import RedisClient

_redis_client = RedisClient()


def start_recommendation_engine() -> None:
    # subscribes to recommendation engine communication channel
    pubsub = _redis_client.connection.pubsub()
    pubsub.psubscribe(**{'re_comm': _request_handler})
    pubsub.run_in_thread(sleep_time=.01)
    print('recommendation engine started')

    # mock loop
    while True:
        mock_user_id = properties.get_random_profile_id()
        print()
        print('sending user id ' + str(mock_user_id))
        _redis_client.connection.publish('ps_comm', dumps({'userId': mock_user_id}))
        sleep(properties.PROFILE_ACQUISITION_FREQUENCY)


def _request_handler(message) -> None:
    message_data = loads(message['data'])
    print('received response ' + str(message_data))
    if message_data['latest']:
        print(message_data)


def _main() -> None:
    start_recommendation_engine()


if __name__ == '__main__':
    _main()
