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


class IngestionPipeline:
    """
    Prepares the Pipeline with list of functions
    """

    def __init__(self, list_of_functions):
        self.list_of_stages = list_of_functions
        self.logger = logger

    def execute_pipe(self, data):
        """
        executes the pipeline and returns the transformed output
        """
        for stage in self.list_of_stages:
            self.logger.info(f"Executing the stage - {stage.__name__}")
            data = stage(data)

        return data


def stage1(df):
    return df.T


def stage2(raw_df):
    df1 = raw_df.drop('info', axis=1).reset_index(drop=True)
    df2 = raw_df['info'].apply(pd.Series).drop('volume', axis=1).reset_index().rename({"index": "symbol"}, axis=1)
    volumne = raw_df['info'].apply(pd.Series)['volume'].apply(pd.Series).reset_index().rename({"index": "symbol"},
                                                                                              axis=1)
    intermediate_df = pd.merge(df1, df2)
    final_df = pd.merge(intermediate_df, volumne)
    return final_df


pipeline_stages = [
    stage1,
    stage2
]

ingestion_pipeline = IngestionPipeline(pipeline_stages)


class Ingestion:
    """
    Reads json/csv data from specified path or filename
    """

    def __init__(self):
        self.logger = logger

    def readFile(self, path: str) -> pd.DataFrame:
        """
        Reads single csv /json files
        """
        filename, file_ext = os.path.splitext(path)
        if file_ext == ".csv":
            df = pd.read_csv(path)
        elif file_ext == ".json":
            df = pd.read_json(path)
        else:
            self.logger.info("Other file formats will be supported later - currently {} not available".format(file_ext))
        return df

    def readFiles(self, list_of_files: List) -> pd.DataFrame:
        """
        Reads multiple json or csv files
        """
        dfs = []
        for f in list_of_files:
            dfs.append(self.readFile(f))
        return pd.concat(dfs)

    def normalize(self, data: pd.DataFrame, pipeline: IngestionPipeline) -> pd.DataFrame:
        """
        Normalize the data (Generic function) based on the given ingestionpipeline
        cleansup the data and returns the normalized dataframe
        """
        data = pipeline.execute_pipe(data)
        return data

    def save_to_db(self):
        return


class IngestionApi:
    def __init__(self, engine, dir=".", ):
        self.dir = dir
        self.engine = engine

    def listdir(self, dir=None):
        if dir:
            return glob.glob(os.path.join(dir, "\*\\"))
        else:
            return glob.glob(os.path.join(self.dir, "\*\\"))

    def listfiles(self, dir, _format="*.json"):
        if dir:
            return glob.glob(os.path.join(dir, _format))
        else:
            return glob.glob(os.path.join(self.dir, _format))

    def drop_table(self, table_name):
        base = declarative_base()
        metadata = MetaData(self.engine, reflect=True)
        table = metadata.tables.get(table_name)
        print(table)
        if table is not None:
            logging.info(f'Deleting {table_name} table')
            base.metadata.drop_all(self.engine, [table], checkfirst=True)

    def file_to_ingest(self, path, table_name: str):
        # get thw data
        logger.info("Getting Raw Data")
        ingestion = Ingestion()
        data = ingestion.readFile(path)

        logger.info("Normalzing the data")
        # normalize
        data = ingestion.normalize(data=data, pipeline=ingestion_pipeline)

        # save the data
        logger.info("Saving the data")
        data.to_sql(table_name, self.engine, if_exists='replace', index=False)

    def dir_to_ingest(self, dirname, _format: str, table_name: str):
        # get the data
        ingestion = Ingestion()
        list_of_files = self.listfiles(dirname, _format)
        data = ingestion.readFiles(list_of_files=list_of_files)

        # normalize
        data = ingestion.normalize(data=data, pipeline=ingestion_pipeline)

        # save the data
        data.to_sql(table_name, self.engine, if_exists='replace', index=False)


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

    def updateTable(self,modified_entry:dict,tablename:str):
        if isinstance(modified_entry, dict):
            modified_entry = [modified_entry]
        logger.info(modified_entry)
        print(modified_entry)
        pd.DataFrame(modified_entry).to_sql(tablename, self.engine, if_exists='append', index = False)

        return "Successfully updated"

    def format(self):
        return


