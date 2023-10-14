"""this file takes a list of tables and compares their column schemas"""

import os
import argparse
from collections import defaultdict
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
import yaml

# from lib.dbtproject import dbtProject
from lib.postgres import get_columnspec as pg_get_columnspec
from lib.bigquery import get_columnspec as bq_get_columnspec


basicConfig(level=INFO)
logger = getLogger()

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
parser.add_argument("--mergespec", required=True)
parser.add_argument("--working-dir", required=True)
parser.add_argument("--threshold", default=0.7, type=float)
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


def get_column_lists(
    p_warehouse: str, p_mergespec: dict, p_connection_info: dict, p_working_dir: str
):
    """gets the column schemas for all tables in the mergespec"""
    column_lists_filename = os.path.join(p_working_dir, "column_lists.yaml")
    if os.path.exists(column_lists_filename):
        with open(column_lists_filename, "r", encoding="utf-8") as column_lists_file:
            column_lists = yaml.safe_load(column_lists_file)
            return column_lists

    column_lists = defaultdict(set)
    for table_iter in p_mergespec["tables"]:
        if p_warehouse == "postgres":
            columns = pg_get_columnspec(
                table_iter["schema"], table_iter["tablename"], p_connection_info
            )
        elif p_warehouse == "bigquery":
            columns = bq_get_columnspec(table_iter["schema"], table_iter["tablename"])

        else:
            raise ValueError("unknown warehouse")

        column_lists[table_iter["tablename"]] = columns

    with open(column_lists_filename, "w", encoding="utf-8") as column_lists_file:
        yaml.dump(dict(column_lists), column_lists_file)

    return column_lists


class Cluster:
    """a set of tables whose columns are similar"""

    def __init__(self, table_to_columns: dict, first_table: str):
        # pylint:disable=invalid-name
        self.T2C = table_to_columns
        self.all_columns = set()
        self.members = set()
        self.add_table(first_table)

    def add_table(self, tablename):
        """updates the cluster with the columns from the given table"""
        self.all_columns.update(self.T2C[tablename])
        self.members.add(tablename)

    def overlap(self, tablename):
        """computes the proportion of overlap between this table's columns and the current
        cluster"""
        these_columns = set(self.T2C[tablename])
        all_columns = set(self.all_columns)
        overlap = (
            1.0 * len(these_columns & all_columns) / len(these_columns | all_columns)
        )
        return overlap

    def print(self):
        """prints the cluster"""
        for tablename in self.members:
            print(f"  {tablename:25}: {self.overlap(tablename):0.2f}")


# -- start
# dbtproject = dbtProject(project_dir)
# dbtproject.ensure_models_dir(mergespec["outputsschema"])

t2c = get_column_lists(warehouse, mergespec, connection_info, working_dir)
# start with an arbitrary table
c1 = Cluster(t2c, mergespec["tables"][0]["tablename"])

for table in mergespec["tables"][1:]:
    if c1.overlap(table["tablename"]) > args.threshold:
        c1.add_table(table["tablename"])

c1.print()