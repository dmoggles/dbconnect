from sqlalchemy import create_engine

USER = "public"

HOST = "5.2.16.131"
DATABASE = "football_data"


def mysql_engine(password, user=USER, host=HOST, port="3306", database=DATABASE):
    engine = create_engine(
        "mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(
            user, password, host, port, database
        )
    )
    return engine
