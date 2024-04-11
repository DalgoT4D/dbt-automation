"""trims tables in the comp schema to line up with the ref schema"""

import os
import sys
import json
import argparse
from logging import basicConfig, getLogger, INFO
import yaml

from dbt_automation.utils.postgres import PostgresClient

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--config-yaml", default="connections.yaml")
parser.add_argument("--working-dir", required=True)
parser.add_argument("--tableroot")
args = parser.parse_args()

with open(args.config_yaml, "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

ref_client = PostgresClient(connection_info["ref"])
comp_client = PostgresClient(connection_info["comp"])

ref_tables = ref_client.get_tables("staging")
comp_tables = comp_client.get_tables("staging")

os.makedirs(args.working_dir + "/destv1v2/", exist_ok=True)


def translate_tablename(destv2_table: str):
    return destv2_table.replace(
        "airbyte_", "_airbyte_raw_"
    ).lower(), destv2_table.replace("airbyte_", "")


def replace_single_quote_with_double_quotes(s: str):
    if isinstance(s, dict):
        return json.dumps(s).replace("'", '"')
    return s.replace("'", '"')


def readable_u2(s: str):
    try:
        return s.encode().decode("unicode_escape")
    except ValueError:
        print(s)
        raise


def readable_x2(s: str):
    try:
        return s  # .encode("latin1").decode("utf-8")
    except ValueError:
        print(s)
        raise


# compare row counts
for comp_tablename in comp_tables:
    ref_tablename, tableroot = translate_tablename(comp_tablename)
    if args.tableroot and tableroot != args.tableroot:
        continue
    if ref_tablename not in ref_tables:
        print("WARNING: ref table not found", ref_tablename)
        continue
    comp_rowcount = int(
        comp_client.execute(f'SELECT COUNT(*) FROM "staging"."{comp_tablename}"')[0][0]
    )
    ref_rowcount = int(
        ref_client.execute(f'SELECT COUNT(*) FROM "staging"."{ref_tablename}"')[0][0]
    )
    if comp_rowcount != ref_rowcount:
        print(f"table_name={comp_tablename}")

        extract_cmd_ref = f"SELECT _airbyte_data->>'data' as data, _airbyte_data->>'id' as id FROM \"staging\".\"{ref_tablename}\" ORDER BY _airbyte_data->>'id'"
        results_ref = ref_client.execute(extract_cmd_ref)
        unique_ids_ref = set([row[1] for row in results_ref])

        extract_cmd_comp = f"SELECT data, data->>'id' as id FROM \"staging\".\"{comp_tablename}\" ORDER BY data->>'id'"
        results_comp = comp_client.execute(extract_cmd_comp)
        unique_ids_comp = set([row[1] for row in results_comp])

        if len(unique_ids_ref.difference(unique_ids_comp)) > 0:
            with open(
                f"{args.working_dir}/destv1v2/{tableroot}_ref.csv",
                "w",
                encoding="utf-8",
            ) as ref_csv:
                for refid in unique_ids_ref.difference(unique_ids_comp):
                    ref_csv.write(refid + "\n")

        if len(unique_ids_comp.difference(unique_ids_ref)) > 0:
            with open(
                f"{args.working_dir}/destv1v2/{tableroot}_comp.csv",
                "w",
                encoding="utf-8",
            ) as comp_csv:
                for compid in unique_ids_comp.difference(unique_ids_ref):
                    comp_csv.write(compid + "\n")
