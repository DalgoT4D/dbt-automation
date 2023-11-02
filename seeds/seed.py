"""This script seeds airbyte's raw data into test warehouse"""

import argparse, os
from logging import basicConfig, getLogger, INFO
from dbt_automation.utils.warehouseclient import get_client
from dotenv import load_dotenv
import csv
from pathlib import Path
import json


basicConfig(level=INFO)
logger = getLogger()

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
args = parser.parse_args()
warehouse = args.warehouse

load_dotenv("dbconnection.env")

tablename = "_airbyte_raw_Sheet1"
json_file = "seeds/sample_sheet1.json"

data = []
with open(json_file, "r") as file:
    data = json.load(file)

columns = ["_airbyte_ab_id", "_airbyte_data", "_airbyte_emitted_at"]

# schema check; expecting only airbyte raw data
for row in data:
    schema_check = [True if key in columns else False for key in row.keys()]
    if all(schema_check) is False:
        raise Exception("Schema mismatch")


if args.warehouse == "postgres":
    logger.info("Found postgres warehouse")
    conn_info = {
        "host": os.getenv("TEST_PG_DBHOST"),
        "port": os.getenv("TEST_PG_DBPORT"),
        "username": os.getenv("TEST_PG_DBUSER"),
        "database": os.getenv("TEST_PG_DBNAME"),
        "password": os.getenv("TEST_PG_DBPASSWORD"),
    }
    schema = os.getenv("TEST_PG_DBSCHEMA")

    wc_client = get_client(args.warehouse, conn_info)

    drop_schema_query = f"""
        DROP SCHEMA IF EXISTS {schema} CASCADE;
    """
    create_schema_query = f"""
        CREATE SCHEMA IF NOT EXISTS {schema};
    """
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}."{tablename}"
        (
            _airbyte_ab_id character varying,
            _airbyte_data jsonb,
            _airbyte_emitted_at timestamp with time zone
        );
    """

    wc_client.runcmd(drop_schema_query)
    wc_client.runcmd(create_schema_query)
    wc_client.runcmd(create_table_query)

    """
    INSERT INTO your_table_name (column1, column2, column3, ...)
    VALUES ({}, {}, {}, ...);
    """
    # seed sample json data into the newly table created
    logger.info("seeding sample json data")
    for row in data:
        # Execute the insert query with the data from the CSV
        insert_query = f"""INSERT INTO {schema}."{tablename}" ({', '.join(columns)}) VALUES ('{row['_airbyte_ab_id']}', JSON '{row['_airbyte_data']}', '{row['_airbyte_emitted_at']}')"""
        wc_client.runcmd(insert_query)

    wc_client.close()
    logger.info("seeding finished")


if args.warehouse == "bigquery":
    logger.info("Found bigquery warehouse")
    conn_info = os.getenv("TEST_BG_SERVICEJSON")
    print(conn_info)
    print(type(conn_info))
