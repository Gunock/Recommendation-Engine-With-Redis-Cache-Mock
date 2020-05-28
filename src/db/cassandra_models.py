from uuid import uuid1

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


class UserProfile(Model):
    __keyspace__ = 'wti_cache'
    __table_name__ = 'user_profiles'
    uuid = columns.UUID(required=True, default=uuid1)
    user_id = columns.Integer(required=True, primary_key=True)
    genre_adventure = columns.Float(required=False, default=0.0)
    genre_animation = columns.Float(required=False, default=0.0)
    genre_children = columns.Float(required=False, default=0.0)
    genre_comedy = columns.Float(required=False, default=0.0)
    genre_fantasy = columns.Float(required=False, default=0.0)
    genre_romance = columns.Float(required=False, default=0.0)
    genre_drama = columns.Float(required=False, default=0.0)
    genre_action = columns.Float(required=False, default=0.0)
    genre_crime = columns.Float(required=False, default=0.0)
    genre_thriller = columns.Float(required=False, default=0.0)
    genre_horror = columns.Float(required=False, default=0.0)
    genre_mystery = columns.Float(required=False, default=0.0)
    genre_sci_fi = columns.Float(required=False, default=0.0)
    genre_imax = columns.Float(required=False, default=0.0)
    genre_documentary = columns.Float(required=False, default=0.0)
    genre_war = columns.Float(required=False, default=0.0)
    genre_musical = columns.Float(required=False, default=0.0)
    genre_film_noir = columns.Float(required=False, default=0.0)
    genre_western = columns.Float(required=False, default=0.0)
    genre_short = columns.Float(required=False, default=0.0)
