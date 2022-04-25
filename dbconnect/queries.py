from typing import List
from pypika import Table, MySQLQuery
from dbconnect.connector import mysql_engine
import pandas as pd

from dbconnect.odm import QueryWrapper
import abc


class DataQuery(abc.ABC):
    def __init__(self, password, **kwargs):
        self._engine = mysql_engine(password=password, **kwargs)

    @abc.abstractmethod
    def query(self, columns: List[str] = None, **kwargs):
        pass


class FbRefQuery(DataQuery):
    def query(self, columns: List[str] = None, **kwargs):
        """
        Get all the data from the fbref table.
        """
        table = Table("fbref2")

        query = MySQLQuery.from_(table)
        if columns:
            query = query.select(*columns)
        else:
            query = query.select(table.star)
        if "season" in kwargs:
            query = query.where(table.season == kwargs["season"])
        if "squad" in kwargs:
            query = query.where(table.squad == kwargs["squad"])
        if "player" in kwargs:
            query = query.where(table.player == kwargs["player"])
        if "opponent" in kwargs:
            query = query.where(table.opponent == kwargs["opponent"])
        if "league" in kwargs:
            query = query.where(table.comp == kwargs["league"])

        return QueryWrapper(query, self._engine)
