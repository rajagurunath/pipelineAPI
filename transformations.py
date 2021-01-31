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


class TransformationApi:
    """
    Custom Transformations and SQL Transformations which will be inserted into
    table
    """

    def __init__(self, engine):
        self.engine = engine

    def trim(self, tablename: str, column_to_trim: str, where_condition: str = None):
        if where_condition is not None:
            insert_stmt = f'''insert into {tablename} ({column_to_trim}) 
                            select lower({column_to_trim}) from {tablename} 
                            where symbol = "{where_condition}" '''
        else:
            insert_stmt = f'''insert into {tablename} ({column_to_trim}) 
                                        select lower({column_to_trim}) from {tablename}'''
        return self.engine.execute(insert_stmt)

    def lower(self, tablename: str, column_to_trim: str, where_condition: str = None):
        if where_condition is not None:
            insert_stmt = f'''insert into {tablename} ({column_to_trim}) 
                                    select trim({column_to_trim}) from {tablename} 
                                    where symbol = "{where_condition}" '''
        else:
            insert_stmt = f'''insert into {tablename} ({column_to_trim}) 
                                                select trim({column_to_trim}) from {tablename} '''
        return self.engine.execute(insert_stmt)

    def updateTable(self, modified_entry: dict, tablename: str):
        if isinstance(modified_entry, dict):
            modified_entry = [modified_entry]
        logger.info(modified_entry)
        print(modified_entry)
        pd.DataFrame(modified_entry).to_sql(tablename, self.engine, if_exists='append', index=False)

        return "Successfully updated"

    def format(self):
        return
