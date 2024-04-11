"""trims tables in the comp schema to line up with the ref schema"""

import sys
import argparse
from logging import basicConfig, getLogger, INFO
import yaml

from dbt_automation.utils.postgres import PostgresClient

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--config-yaml", default="connections.yaml")
parser.add_argument("--ref-sources-yaml", required=True)
parser.add_argument("--comp-sources-yaml", required=True)
args = parser.parse_args()

with open(args.config_yaml, "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

with open(args.ref_sources_yaml, "r", encoding="utf-8") as ref_sources_yaml:
    ref_sources = yaml.safe_load(ref_sources_yaml)

with open(args.comp_sources_yaml, "r", encoding="utf-8") as comp_sources_yaml:
    comp_sources = yaml.safe_load(comp_sources_yaml)

ref_client = PostgresClient(connection_info["ref"])
comp_client = PostgresClient(connection_info["comp"])

ref_tables = ref_client.get_tables("staging")
comp_tables = comp_client.get_tables("staging")

# ref_tables are of the form _airbyte_raw_*
# containing the columns { _airbyte_ab_id, _airbyte_data, _airbyte_emitted_at }
# comp_tables are of the form <prefix>tablename
# containg the columns { _airbyte_extracted_at, _airbyte_raw_id, ... }


def schematable2source(sourceschema, sourcetable, sources: dict):
    """returns the source and table name from the schema and table name"""
    for source in sources["sources"]:
        if source["schema"] == sourceschema:
            for table in source["tables"]:
                if table["identifier"] == sourcetable:
                    return {
                        "sourceschema": source["name"],
                        "sourcetable": table["name"],
                    }
    return None


def source2schematable(sourceschema, sourcetable, sources: dict):
    """returns the schema and table name from the source and table name"""
    for source in sources["sources"]:
        if source["name"] == sourceschema:
            for table in source["tables"]:
                if table["name"] == sourcetable:
                    return {"schema": source["schema"], "table": table["identifier"]}
    return None


def translate_tablename(tablename, from_sources, to_sources):
    """looks up the tablename in the ref_sources and comp_sources and returns the comp tablename"""
    comp_source = schematable2source("staging", tablename, from_sources)
    if comp_source:
        ref_source = source2schematable(
            comp_source["sourceschema"], comp_source["sourcetable"], to_sources
        )
        if ref_source:
            # print(
            #     f"schema={"staging":20} table={tablename:60} schema={ref_source['schema']:20} table={ref_source['table']:40}"
            # )
            return ref_source["table"]


# compare row counts
for comp_table in comp_tables:
    ref_table = translate_tablename(comp_table, comp_sources, ref_sources)
    if ref_table:
        comp_rowcount = int(
            comp_client.execute(f'SELECT COUNT(*) FROM staging."{comp_table}"')[0][0]
        )
        ref_rowcount = int(
            ref_client.execute(f'SELECT COUNT(*) FROM staging."{ref_table}"')[0][0]
        )
        pctg_diff = abs(comp_rowcount - ref_rowcount) / ref_rowcount
        if comp_rowcount != ref_rowcount:
            print(
                f"{comp_table:60} {ref_table:60} comp_rowcount={comp_rowcount:6} ref_rowcount={ref_rowcount:6} pctg_diff={pctg_diff:0.2%}"
            )

sys.exit(0)

# for comp_table in comp_tables:
#     if comp_table == "airbyte_zzz_case":
#         ref_table = translate_tablename(comp_table, comp_sources, ref_sources)
#         if ref_table:
#             # print(f"comp_table={comp_table:60} ref_table={ref_table:60}")
#             # find an id column in the comp_table
#             comp_columns = comp_client.get_columnspec("staging", comp_table)
#             comp_date_modified = comp_client.execute(
#                 f'SELECT MIN("data"::json->>\'date_modified\'), MAX("data"::json->>\'date_modified\') FROM staging."{comp_table}"'
#             )[0]
#             ref_date_modified = ref_client.execute(
#                 f"SELECT MIN(\"_airbyte_data\"::json->'data'->>'date_modified'), MAX(\"_airbyte_data\"::json->'data'->>'date_modified') FROM staging.\"{ref_table}\""
#             )[0]
#             # print(
#             #     f"{comp_table:60} {ref_table:60} {comp_date_modified[0].strftime('%Y-%m-%dT%H:%M:%S.%f')}, {ref_date_modified[0]}"
#             # )
#             # print(
#             #     f"{comp_table:60} {ref_table:60} {comp_date_modified[1].strftime('%Y-%m-%dT%H:%M:%S.%f')}, {ref_date_modified[1]}"
#             # )
#             delete_cmd = f"DELETE FROM staging.\"{comp_table}\" WHERE \"data\"::json->>'date_modified' < '{ref_date_modified[0]}' OR \"data\"::json->>'date_modified' > '{ref_date_modified[1]}'"
#             print(delete_cmd)
#             try:
#                 comp_client.runcmd(delete_cmd)
#             except Exception as e:
#                 print(e)
#     else:
#         ref_table = translate_tablename(comp_table, comp_sources, ref_sources)
#         if ref_table:
#             # print(f"comp_table={comp_table:60} ref_table={ref_table:60}")
#             # find an id column in the comp_table
#             comp_columns = comp_client.get_columnspec("staging", comp_table)
#             comp_indexed_on = comp_client.execute(
#                 f'SELECT MIN(indexed_on), MAX(indexed_on) FROM staging."{comp_table}"'
#             )[0]
#             ref_indexed_on = ref_client.execute(
#                 f'SELECT MIN("_airbyte_data"::json->>\'indexed_on\'), MAX("_airbyte_data"::json->>\'indexed_on\') FROM staging."{ref_table}"'
#             )[0]
#             # print(
#             #     f"{comp_table:60} {ref_table:60} {comp_indexed_on[0].strftime('%Y-%m-%dT%H:%M:%S.%f')}, {ref_indexed_on[0]}"
#             # )
#             # print(
#             #     f"{comp_table:60} {ref_table:60} {comp_indexed_on[1].strftime('%Y-%m-%dT%H:%M:%S.%f')}, {ref_indexed_on[1]}"
#             # )
#             delete_cmd = f"DELETE FROM staging.\"{comp_table}\" WHERE indexed_on < '{ref_indexed_on[0]}' OR indexed_on > '{ref_indexed_on[1]}'"
#             print(delete_cmd)
#             try:
#                 comp_client.runcmd(delete_cmd)
#             except Exception as e:
#                 pass
