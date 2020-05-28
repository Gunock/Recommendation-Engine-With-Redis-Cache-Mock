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

        # creates tables if non existent
        management.sync_table(UserProfile)

    def __del__(self):
        if self.session is not None:
            self.session.shutdown()
        if self.cluster is not None:
            self.cluster.shutdown()

    def profile_exists(self, user_id: int) -> bool:
        return len(UserProfile.filter(user_id=user_id).all()) != 0

    def get_profile_uuid(self, user_id: int) -> str:
        if not self.profile_exists(user_id):
            return ''
        return str(UserProfile.filter(user_id=user_id).all()[0].uuid)

    def get_profile_if_updated(self, user_id: int, uuid: str) -> dict:
        if not self.profile_exists(user_id):
            return {}
        result_model: UserProfile = UserProfile.filter(user_id=user_id).all()[0]
        if str(result_model.uuid) == uuid:
            return {}

        result = CassandraClient._replace_underscores(dict(result_model))
        result['uuid'] = str(result['uuid'])
        return CassandraClient._round_floats(result)

    def get_profile(self, user_id: int) -> dict:
        if not self.profile_exists(user_id):
            return {}
        result = dict(UserProfile.filter(user_id=user_id).all()[0])
        result['uuid'] = str(result['uuid'])
        result = CassandraClient._replace_underscores(result)
        return CassandraClient._round_floats(result)

    def add_profile(self, user_id: int, profile_json: dict) -> None:
        profile = UserProfile(user_id=user_id)
        for key in profile_json:
            key: str = key
            exec('profile.' + key.lower().replace('-', '_') + ' = ' + str(profile_json[key]))
        profile.save()

    def remove_profile(self, user_id: int) -> dict:
        profile = UserProfile(user_id=user_id)
        result = dict(profile)
        profile.delete()
        return result

    @staticmethod
    def _replace_underscores(input_json: dict):
        keys: list = list(input_json.keys())
        for key in keys:
            key: str = key
            if '_' in key:
                input_json[key.replace('_', '-')] = input_json[key]
                del input_json[key]
        return input_json

    @staticmethod
    def _round_floats(input_json: dict) -> dict:
        for key in input_json:
            if type(input_json[key]) != float:
                continue
            input_json[key] = round(input_json[key], 6)
        return input_json
