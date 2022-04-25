from pypika import Query
from sqlalchemy.engine import Engine
import pandas as pd


class QueryWrapper:
    def __init__(self, query: Query, engine: Engine) -> None:
        self.query = query
        self.engine = engine

    def get(self) -> pd.DataFrame:
        return pd.read_sql(self.query.get_sql(), self.engine)
