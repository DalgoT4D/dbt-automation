"""given a table, dorp columns which have few distinct values"""

import os
import argparse
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
import yaml

# from lib.dbtproject import dbtProject
from lib.postgres import get_columnspec as pg_get_columnspec, get_connection
from lib.bigquery import get_columnspec as bq_get_columnspec
from google.cloud import bigquery

basicConfig(level=INFO)
logger = getLogger()

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
parser.add_argument("--mergespec", required=True)
parser.add_argument("--working-dir", required=True)
args = parser.parse_args()

# ================================================================================
load_dotenv("dbconnection.env")
project_dir = os.getenv("DBT_PROJECT_DIR")
warehouse = args.warehouse
working_dir = args.working_dir

connection_info = {
    "DBHOST": os.getenv("DBHOST"),
    "DBPORT": os.getenv("DBPORT"),
    "DBUSER": os.getenv("DBUSER"),
    "DBPASSWORD": os.getenv("DBPASSWORD"),
    "DBNAME": os.getenv("DBNAME"),
}

with open(args.mergespec, "r", encoding="utf-8") as mergespecfile:
    mergespec = yaml.safe_load(mergespecfile)


for table in mergespec["tables"]:
    tablename = table["tablename"]

    if warehouse == "postgres":
        columnspec = pg_get_columnspec(
            table["schema"], table["tablename"], connection_info
        )
    elif warehouse == "bigquery":
        columnspec = bq_get_columnspec(
            table["schema"],
            table["tablename"],
        )
    else:
        raise ValueError("invalid warehouse")

    for column in columnspec:
        statement = f"SELECT COUNT (DISTINCT {column}) FROM {table['schema']}.{table['tablename']}"
        if warehouse == "postgres":
            with get_connection(
                connection_info["DBHOST"],
                connection_info["DBPORT"],
                connection_info["DBUSER"],
                connection_info["DBPASSWORD"],
                connection_info["DBNAME"],
            ) as conn:
                cursor = conn.cursor()
                cursor.execute(statement)
                result = cursor.fetchone()
                print(tablename, column, result)

        elif warehouse == "bigquery":
            client = bigquery.Client()
            query_job = client.query(statement)
            result = query_job.result()
            for row in result:
                print(tablename, column, row[0])
        else:
            raise ValueError("invalid warehouse")
