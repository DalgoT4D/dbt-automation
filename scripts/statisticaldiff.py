""" for every table in the given schema across two databases, compare the columns' uiqueness-index """

import argparse
from logging import basicConfig, getLogger, INFO
import yaml

from dbt_automation.utils.postgres import PostgresClient

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--config-yaml", default="connections.yaml")
parser.add_argument("--schema", required=True)
args = parser.parse_args()

with open(args.config_yaml, "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

ref_client = PostgresClient(connection_info["ref"])
comp_client = PostgresClient(connection_info["comp"])

ref_tables = ref_client.get_tables(args.schema)
comp_tables = comp_client.get_tables(args.schema)


for table in comp_tables:
    comp_columns = comp_client.get_table_columns(args.schema, table)
    ref_columns = ref_client.get_table_columns(args.schema, table)

    for column in comp_columns:
        if column not in ref_columns:
            if column not in ["_airbyte_raw_id", "_airbyte_extracted_at"]:
                print(f"column {column} not in ref <============= ")
            continue

        comp_total = comp_client.execute(
            f"SELECT COUNT({column}) FROM {args.schema}.{table}"
        )[0][0]
        ref_total = ref_client.execute(
            f"SELECT COUNT({column}) FROM {args.schema}.{table}"
        )[0][0]

        if comp_total == 0 or ref_total == 0:
            continue

        comp_unique = comp_client.execute(
            f"SELECT COUNT(DISTINCT {column}) FROM {args.schema}.{table}"
        )[0][0]
        ref_unique = ref_client.execute(
            f"SELECT COUNT(DISTINCT {column}) FROM {args.schema}.{table}"
        )[0][0]

        if comp_total == 0:
            print(
                f"comp_total=0 for column {column} ref_total={ref_total} <================== "
            )
            continue
        elif ref_total == 0:
            print(
                f"ref_total=0 for column {column} comp_total={comp_total} <================== "
            )
            continue

        comp_uniqueness = comp_unique / comp_total
        ref_uniqueness = ref_unique / ref_total

        if abs(comp_uniqueness - ref_uniqueness) > 0.01:
            print(
                f"{table:60} {column:60} comp_uniqueness={comp_uniqueness:.2f} ref_uniqueness={ref_uniqueness:.2f}"
            )
