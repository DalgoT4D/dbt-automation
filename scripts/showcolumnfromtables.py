"""given a list of tables, show the unique values of the specified column"""

import argparse
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
from dbt_automation.lib.warehouseclient import get_client

load_dotenv("dbconnection.env")

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
parser.add_argument("--schema", required=True)
parser.add_argument("--column", required=True)
parser.add_argument("--tables", nargs="+", required=True)
args = parser.parse_args()

warehouse = args.warehouse
schema = args.schema
column = args.column

# -- start
client = get_client(warehouse)

for tablename in args.tables:
    QUERY = f"SELECT DISTINCT {column} FROM {schema}.{tablename}"
    resultset = client.execute(QUERY)
    for result in resultset:
        if warehouse == "bigquery":
            print(result[column])
        else:
            print(result[0])
