from typing import List
from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session
from ingestion import IngestionApi
import models, schemas
from transformations import TransformationApi
from dataAvailability import DataQueryApi
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Data Pipeline")

default_data_path = "data/"
ingestion_api = IngestionApi(engine, default_data_path)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/listdir", tags=["Ingestion"])
def listdir(dirname: str):
    """
    # list Directory in a root directory
    """
    return ingestion_api.listdir(dirname)


@app.get("/listfiles", tags=["Ingestion"])
def listfiles(dirname: str, file_format: str):
    """
    # List files in a given directory
    - dirname
    - file_format : *.json/*.csv
    """
    return ingestion_api.listfiles(dirname, _format=file_format)


@app.get("/ingestFile", tags=["Ingestion"])
def ingestFile(file_path: str, table_name_to_ingest: str):
    """
    # Ingest Single file into a Database

    - file path to ingest
    - table_name_to_ingest : (create the table if it doesn't exist or replace it )
    """
    try:
        ingestion_api.file_to_ingest(file_path, table_name=table_name_to_ingest)
    except Exception as e:
        return {"data": f"Error occured in the ingesting the file - {e}"}
    return "Data successfully Ingested"


@app.get("/ingestListFiles", tags=["Ingestion"])
def ingestListFiles(dirname: str, file_format: str, table_name_to_ingest: str):
    """
    ## ingest List of Files into DB

    ### input args :

        - dirname : path to directory
        - file_format : *.json/*.csv
        - table_name_to_ingest : sql table name
    """
    try:
        ingestion_api.dir_to_ingest(dirname, _format=file_format, table_name=table_name_to_ingest)
    except Exception as e:
        return {"data": f"Error occured in the ingesting the file - {e}"}
    return "Data successfully Ingested"


@app.get("/deleteTable", tags=["Ingestion"])
def deleteTable(table_name_to_drop: str):
    """
    # Delete the Table :

    input args :
    - table_name_to_drop
    """
    return ingestion_api.drop_table(table_name=table_name_to_drop)


@app.post("/lowerCase", tags=["Transformations"])
def lowerCase(tablename: str, column_to_lowercase: str, where_condition: str = None):
    """
    Lowercase the specified column and insert into the specified Table

    - tablename
    - column_to_lowercase
    - where_condition  : can be not specified also
        - (if where condition is None it will update entire Table wherever applicable)
    """
    transformation = TransformationApi(engine)
    try:
        transformation.lower(tablename, column_to_lowercase, where_condition=where_condition)
    except Exception as e:
        return {"data": f"Error occured in the ingesting the file - {e}"}

    return "Data Successfully Inserted"


@app.post("/trim", tags=["Transformations"])
def trimming(tablename: str, column_to_trim: str, where_condition: str = None):
    """
    Trim (Left/Right space) in the specified column and insert into the specified Table

    - tablename
    - column_to_lowercase
    - where_condition  : can be not specified also
        - (if where condition is None it will update entire Table wherever applicable)
    """
    transformation = TransformationApi(engine)
    try:
        transformation.trim(tablename, column_to_trim, where_condition=where_condition)
    except Exception as e:
        return {"data": f"Error occured in the ingesting the file - {e}"}
    return "Data Successfully Inserted"


@app.post("/updateTable", tags=["Transformations"])
def updateTable(tablename: str, data: List[dict], ):
    """
    Update the Table (this API can be used to update the table using modified (trimmed/formatted)
    data into database (from third party Applications)

    - tablename
    - modified data
        - example:
        ```
        modified_entry = [{
            "symbol": "BTC",
            "timestamp": 1602485335788,
            "datetime": 1602485335788,
            "high": "874081.00",
            "low": "853002.96",
            "bid": 858558.95,
            "bidVolume": "",
            "ask": 859172.24,
            "askVolume": "",
            "vwap": "",
            "open": 860330.8,
            "close": 858586.59,
            "last": 858586.59,
            "baseVolume": 5.99453366,
            "quoteVolume": "",
            "previousClose": "",
            "change": -1744.2100000000792,
            "percentage": -0.20273713320505002,
            "average": 859458.6950000001,
            "highest_buy_bid": 858558.95,
            "lowest_sell_bid": 859172.24,
            "last_traded_price": 858586.59,
            "yes_price": 860330.8,
            "inr_price": "np.nan",
            "max": "874081.00",
            "min": "853002.96",
            "volume": 5.99453366
        }]
    ```
    """
    transformation = TransformationApi(engine)
    try:
        transformation.updateTable(data, tablename=tablename)
    except Exception as e:
        return {"data": f"Error Ocuured while updating the Table - {e}"}
    return {"data": "Successfully updated"}


@app.get("/listTables", tags=["DataAvailability"])
def listTables():
    """
    # List Tables present in the Database
    """
    dataq = DataQueryApi(engine)
    return dataq.listTables()


@app.post("/query_data", tags=["DataAvailability"])
def query_data(sql_query: str):
    """
    # Uses Sql to Query the Backend Database

    example :
     - "select * from {table_name} limit 5"
     - "select * from test limit 5"
     - select * from fetchTickers limit 5

    """
    dataq = DataQueryApi(engine)
    try:
        res = dataq.execute_sql(sql_query)
    except Exception as e:
        return {"data": f"Error Occured while Querying the data - {e}"}
    return res
