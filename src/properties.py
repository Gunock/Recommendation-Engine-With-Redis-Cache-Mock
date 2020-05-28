import os
import random

REDIS_HOST = str(os.environ.get('wti.redisAddress', default='localhost'))
REDIS_PORT = int(os.environ.get('wti.redisPort', default=6379))
CASSANDRA_HOST = str(os.environ.get('wti.cassandraAddress', default='localhost'))
CASSANDRA_PORT = int(os.environ.get('wti.cassandraPort', default=9042))

PROFILE_COUNT = int(os.environ.get('wti.profileCount', default=100))
PROFILE_UPDATE_FREQUENCY = int(os.environ.get('wti.profileUpdateFrequency', default=2))
PROFILE_ACQUISITION_FREQUENCY = int(os.environ.get('wti.profileAcquisitionFrequency', default=1))

# Update timeout in seconds
PROFILE_UPDATE_TIMEOUT = float(os.environ.get('wti.profileAcquisitionTimeout', default=0.1))

# Random delay in milliseconds
_MIN_RANDOM_DELAY = int(int(os.environ.get('wti.maxRandomDelay', default=50)))
_MAX_RANDOM_DELAY = int(int(os.environ.get('wti.minRandomDelay', default=200)))


def get_random_profile_id() -> int:
    return random.randint(1, PROFILE_COUNT)


def get_random_delay_millis() -> int:
    return random.randint(_MIN_RANDOM_DELAY, _MAX_RANDOM_DELAY)


def get_random_delay_secs() -> float:
    return get_random_delay_millis() / 1000
