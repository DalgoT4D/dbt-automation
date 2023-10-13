"""takes a list of tables and a common column spec and creates a dbt model to merge them"""
import os
import sys

import argparse
from collections import Counter
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
import yaml

from lib.dbtproject import dbtProject
from lib.dbtconfigs import get_columns_from_model


basicConfig(level=INFO)
logger = getLogger()


# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--models", required=True, help="models.yml")
parser.add_argument("--mergespec", required=True)
parser.add_argument("--warehouse", choices=["postgres", "bigquery"], required=True)
args = parser.parse_args()

# ================================================================================
load_dotenv("dbconnection.env")

project_dir = os.getenv("DBT_PROJECT_DIR")


def fmt_colname(colname: str):
    """format a column name for the target warehouse"""
    if args.warehouse == "postgres":
        return '"' + colname + '"'
    elif args.warehouse == "bigquery":
        return colname.lower()
    else:
        raise ValueError(f"unknown warehouse: {args.warehouse}")


# ================================================================================
with open(args.mergespec, "r", encoding="utf-8") as mergespecfile:
    mergespec = yaml.safe_load(mergespecfile)

table_counts = Counter([m["tablename"] for m in mergespec["tables"]])
has_error: bool = False
for tablename, tablenamecount in table_counts.items():
    if tablenamecount > 1:
        logger.error("table appears more than once in spec: %s", tablename)
        has_error = True
if has_error:
    sys.exit(1)

dbtproject = dbtProject(project_dir)
dbtproject.ensure_models_dir(mergespec["outputsschema"])

with open(args.models, "r", encoding="utf-8") as modelfile:
    models = yaml.safe_load(modelfile)

all_columns = set()
for table in mergespec["tables"]:
    columns = get_columns_from_model(models, table["tablename"])
    all_columns = all_columns.union(columns)

for table in mergespec["tables"]:
    columns = get_columns_from_model(models, table["tablename"])
    statement: str = "{{ config(materialized='table',) }}\nSELECT "
    for column in all_columns:
        if column in columns:
            statement += f"{fmt_colname(column)} AS {fmt_colname(column)},"
        else:
            statement += f"NULL AS {fmt_colname(column)},"
    statement = statement[:-1]  # drop the final comma
    statement += f" FROM {{{{ref('{table['tablename']}')}}}}"
    dbtproject.write_model(
        mergespec["outputsschema"],
        f'premerge_{table["tablename"]}',
        statement,
        subdir="premerge",
    )

# pylint:disable=invalid-name
relations = "["
for table in mergespec["tables"]:
    relations += f"ref('premerge_{table['tablename']}'),"
relations = relations[:-1]
relations += "]"
union_code = "{{ config(materialized='table',) }}\n"
# pylint:disable=consider-using-f-string
union_code += "{{ dbt_utils.union_relations("
union_code += f"relations={relations}"
union_code += ")}}"
dbtproject.write_model(
    mergespec["outputsschema"],
    f"merged_{mergespec['mergename']}",
    union_code,
)
