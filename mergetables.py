"""takes a list of tables and a common column spec and creates a dbt model to merge them"""
import os
import sys

import argparse
from collections import Counter
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
import yaml

from lib.dbtproject import dbtProject


basicConfig(level=INFO)
logger = getLogger()


# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--mergespec", required=True)
args = parser.parse_args()

# ================================================================================
load_dotenv("dbconnection.env")
project_dir = os.getenv("DBT_PROJECT_DIR")

# ================================================================================
with open(args.mergespec, "r", encoding="utf-8") as mergespecfile:
    mergespec = yaml.safe_load(mergespecfile)

table_counts = Counter([m["tablename"] for m in mergespec["tables"]])
# pylint:disable=invalid-name
has_error = False
for tablename, tablenamecount in table_counts.items():
    if tablenamecount > 1:
        logger.error("table appears more than once in spec: %s", tablename)
        has_error = True
if has_error:
    sys.exit(1)

dbtproject = dbtProject(project_dir)
dbtproject.ensure_models_dir(mergespec["outputsschema"])

# pylint:disable=invalid-name
relations = "["
for table in mergespec["tables"]:
    relations += f"ref('{table['tablename']}'),"
relations = relations[:-1]
relations += "]"
union_code = "{{ config(materialized='table',) }}\n"
# pylint:disable=consider-using-f-string
union_code += "{{ dbt_utils.union_relations("
union_code += f"relations={relations}"
union_code += ")}}"
dbtproject.write_model(
    mergespec["outputsschema"],
    mergespec["mergename"],
    union_code,
)
