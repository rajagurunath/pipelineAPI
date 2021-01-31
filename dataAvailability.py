from sqlalchemy.orm import Session
import os
from typing import List
import glob
import models, schemas
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
import logging

logger = logging.getLogger(__name__)


class DataQueryApi:
    def __init__(self, engine):
        self.engine = engine

    def listTables(self):
        base = declarative_base()
        metadata = MetaData(self.engine, reflect=True)
        list_of_tables = metadata.tables.keys()
        print(list_of_tables)
        return list(list_of_tables)

    def execute_sql(self, sql: str):
        df = pd.read_sql(sql, self.engine)
        return df.to_dict(orient='rows')



