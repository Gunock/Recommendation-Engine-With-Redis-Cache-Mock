from cassandra.cluster import Cluster, Session
from cassandra.cqlengine import connection, management

from src.db.cassandra_models import UserProfile
from src.db.db_client import DbClient


class CassandraClient(DbClient):
    def __init__(self, host: str = 'localhost', port: int = 9042):
        self.cluster: Cluster = Cluster([host], port=port)
        self.session: Session = self.cluster.connect()
        connection.set_session(self.session)
        management.create_keyspace_simple(UserProfile.__keyspace__, replication_factor=1)
        connection.session.set_keyspace(UserProfile.__keyspace__)

        # Creates tables if non existent
        management.sync_table(UserProfile)

    def __del__(self):
        if self.session is not None:
            self.session.shutdown()
        if self.cluster is not None:
            self.cluster.shutdown()

    def profile_exists(self, user_id: int) -> bool:
        """
        Check if profile exists in Cassandra database.

        :param user_id: userId of profile to be checked
        :return True if profile exists in database, otherwise False
        """
        return len(UserProfile.filter(user_id=user_id).all()) != 0

    def get_profile(self, user_id: int) -> dict:
        """
        Retrieves profile from Cassandra database.

        :param user_id: userId of profile to be retrieved
        :return profile if exists, otherwise empty dict
        """
        if not self.profile_exists(user_id):
            return {}
        result = dict(UserProfile.filter(user_id=user_id).all()[0])
        result = CassandraClient._replace_underscores(result)
        return CassandraClient._round_floats(result)

    def add_profile(self, user_id: int, profile_json: dict) -> None:
        """
        Save profile in Cassandra database.

        :param user_id: userId of profile to be saved
        :param profile_json: profile details
        :return None
        """
        profile = UserProfile(user_id=user_id)
        for key in profile_json:
            key: str = key
            exec('profile.' + key.lower().replace('-', '_') + ' = ' + str(profile_json[key]))
        profile.save()

    def remove_profile(self, user_id: int) -> None:
        """
        Removes profile from Cassandra database.

        :param user_id:
        :return None
        """
        UserProfile(user_id=user_id).delete()

    @staticmethod
    def _replace_underscores(input_json: dict) -> dict:
        """
        Replaces underscores with dashes.

        :param input_json: Profile with possible underscores in keys
        :return: Profile underscores replaced with dashes in keys
        """
        keys: list = list(input_json.keys())
        for key in keys:
            key: str = key
            if '_' in key:
                input_json[key.replace('_', '-')] = input_json[key]
                del input_json[key]
        return input_json

    @staticmethod
    def _round_floats(input_json: dict) -> dict:
        """
        Rounds float numbers in profile to 6 digit precision.

        :param input_json: Profile with not rounded float numbers
        :return: Profile with rounded float numbers
        """
        for key in input_json:
            if type(input_json[key]) != float:
                continue
            input_json[key] = round(input_json[key], 6)
        return input_json
