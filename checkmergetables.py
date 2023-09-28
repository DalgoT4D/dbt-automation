"""takes a list of tables and a common column spec and creates a dbt model to merge them"""
import os
import sys
import argparse
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
import yaml
from tqdm import tqdm

from lib.postgres import get_columnspec as db_get_colspec, get_connection

basicConfig(level=INFO)
logger = getLogger()


# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--project-dir", required=True)
parser.add_argument("--mergespec", required=True)
args = parser.parse_args()

# ================================================================================
load_dotenv("dbconnection.env")
connection_info = {
    "DBHOST": os.getenv("DBHOST"),
    "DBPORT": os.getenv("DBPORT"),
    "DBUSER": os.getenv("DBUSER"),
    "DBPASSWORD": os.getenv("DBPASSWORD"),
    "DBNAME": os.getenv("DBNAME"),
}


def get_columnspec(schema_: str, table_: str):
    """get the column schema for this table"""
    return db_get_colspec(
        schema_,
        table_,
        connection_info,
    )


# ================================================================================
with open(args.mergespec, "r", encoding="utf-8") as mergespecfile:
    mergespec = yaml.safe_load(mergespecfile)

with get_connection(
    connection_info["DBHOST"],
    connection_info["DBPORT"],
    connection_info["DBUSER"],
    connection_info["DBPASSWORD"],
    connection_info["DBNAME"],
) as conn:
    for table in mergespec["tables"]:
        logger.info("table=%s.%s", table["schema"], table["tablename"])
        columns = get_columnspec(table["schema"], table["tablename"])
        quoted_column_names = [f'"{c}"' for c in columns]
        columnlist = ", ".join(quoted_column_names)
        statement = f"SELECT {columnlist} FROM {table['schema']}.{table['tablename']}"
        # logger.info(statement)
        cursor = conn.cursor()
        cursor.execute(statement)
        resultset = cursor.fetchall()
        for row in tqdm(resultset, desc="Checking table ..."):
            check_statement = "SELECT COUNT(1) FROM "
            check_statement += (
                f"{mergespec['outputsschema']}.merged_{mergespec['mergename']} "
            )
            check_statement += "WHERE "
            filters = []
            for colname, colvalue in zip(columns, row):
                if colvalue is None:
                    filters.append(f'"{colname}" IS NULL')
                elif colvalue.find(chr(39)) < 0:
                    filters.append(f"\"{colname}\" = '{colvalue}'")

            check_statement += " AND ".join(filters)
            # logger.info(check_statement)
            cursor.execute(check_statement)
            check_results = cursor.fetchall()
            if check_results[0][0] != 1:
                logger.error(
                    "check failed, got %d results instead of %d", check_results[0][0], 1
                )
                logger.error(check_statement)
                sys.exit(1)
        cursor.close()
