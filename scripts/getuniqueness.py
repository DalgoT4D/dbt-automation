""" for every table in the given schema across two databases, compare the columns' uiqueness-index """

import argparse
from logging import basicConfig, getLogger, INFO
import yaml

from dbt_automation.utils.postgres import PostgresClient

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--config-yaml", default="connections.yaml")
parser.add_argument("--profile", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--column", required=True)
args = parser.parse_args()

with open(args.config_yaml, "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

client = PostgresClient(connection_info[args.profile])

tables = client.get_tables(args.schema)

for table in tables:
    columns = client.get_table_columns(args.schema, table)

    for column in columns:
        total = client.execute(f"SELECT COUNT({column}) FROM {args.schema}.{table}")[0][
            0
        ]
        unique = client.execute(
            f"SELECT COUNT(DISTINCT {column}) FROM {args.schema}.{table}"
        )[0][0]

        if total == 0:
            print(f"total=0 for {table}.{column} <================== ")
            continue

        uniqueness = unique / total
        if uniqueness < 0.1:
            print(f"{table:50}.{column:20} uniqueness={uniqueness}")
