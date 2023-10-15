from sqlalchemy import create_engine
import pandas as pd
from footmav.data_definitions.whoscored.constants import EventType
import json

USER = "public"

HOST = "5.2.16.131"
DATABASE = "football_data"


def mysql_engine(password, user=USER, host=HOST, port="3306", database=DATABASE, connector='mysqlclient'):
    if connector=='mysqlclient':
        engine = create_engine(
            "mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(
                user, password, host, port, database
            )
        )
    else:
        engine = create_engine(
            "mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(
                user, password, host, port, database
            )
        )
    return engine


class Connection:
    def __init__(self, password, user=USER, host=HOST, port="3306", database=DATABASE):
        self.engine = mysql_engine(password, user, host, port, database)
        
    def query(self, query):
        return pd.read_sql(query, self.engine)

    def wsquery(self, query):
        data = pd.read_sql(query, self.engine)
        data['event_type'] = data['event_type'].apply(lambda x: EventType(x))
        data['qualifiers'] = data['qualifiers'].apply(lambda x: json.loads(x))
        return data
        
    def __del__(self):
        self.engine.dispose()
