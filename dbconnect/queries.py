from typing import List
from pypika import Table, MySQLQuery, Criterion
from dbconnect.connector import mysql_engine
import pandas as pd
from pypika import functions as fn
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


class WhoscoredQuery(DataQuery):
    def query(self, columns: List[str] = None, distinct=False, **kwargs):
        """
        Get all teh data from whoscored table
        """
        table = Table("whoscored")

        query = MySQLQuery.from_(table)
        if columns:
            query = query.select(*columns)
        else:
            query = query.select(table.star)
        if distinct:
            query = query.distinct()
        criterions = []
        if 'players' in kwargs:
            criterions.append(table.player.isin(kwargs['players']))
        if "season" in kwargs:
            criterions.append(table.season == kwargs["season"])
        if "team" in kwargs:
            criterions.append(table.team == kwargs["team"])
        if "player_name" in kwargs:
            criterions.append(table.player == kwargs["player_name"])
        if "opponent" in kwargs:
            criterions.append(table.opponent == kwargs["opponent"])
        if "league" in kwargs:
            criterions.append(table.competition == kwargs["league"])
        if 'match_ids' in kwargs:
            criterions.append(table.matchid.isin(kwargs['match_ids']))
        if 'date' in kwargs:
            if isinstance(kwargs['date'], str):
                if kwargs['date'] == 'latest':
                    criterions.append(table.match_date.isin(
                        table.select(fn.Max(table.match_date)).where(Criterion.all(criterions))))
                else:
                    criterions.append(table.match_date == kwargs['date'])
            else:
                criterions.append(table.match_date.between(
                    kwargs['date'][0], kwargs['date'][1]))
        if criterions:
            query = query.where(Criterion.all(criterions))
        return QueryWrapper(query, self._engine)


class WhoscoredMetaQuery(DataQuery):
    def query(self, match_ids: List[str] = None):
        """
        Get all the data from the whoscored meta table.
        """
        table = Table("whoscored_meta")

        query = MySQLQuery.from_(table).select(table.star)
        if match_ids:
            query = query.where(table.matchid.isin(match_ids))
        return QueryWrapper(query, self._engine)


class WhoscoredPositions(DataQuery):
    def query(self):
        """
        Get all the data from the whoscored positions table.
        """
        table = Table("whoscored_positions")

        query = MySQLQuery.from_(table).select(table.star)
        return QueryWrapper(query, self._engine)
