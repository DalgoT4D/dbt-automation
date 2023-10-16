"""this file takes a list of tables and compares their column schemas"""

import os
import argparse
from collections import defaultdict
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
import yaml
import pandas as pd

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

    def __init__(self, table_to_columns: dict, first_table: str, threshold: float):
        # pylint:disable=invalid-name
        self.T2C = table_to_columns
        self.all_columns = set()
        self.members = set()
        self.threshold = threshold
        self.add_table(first_table)
        self.first_table = first_table

    def add_table(self, tablename):
        """updates the cluster with the columns from the given table"""
        self.all_columns.update(self.T2C[tablename])
        self.members.add(tablename)

    def overlap_X(self, tablename):  # pylint:disable=invalid-name
        """rerturns the proportion of the current column set that is in this table"""
        columns_to_add = set(self.T2C[tablename])
        current_columns = set(self.all_columns)
        intersection = (
            1.0 * len(columns_to_add & current_columns) / len(current_columns)
        )
        # print(f"table: {tablename} intersection: {intersection}")
        return intersection

    def overlap_U(self, tablename):  # pylint:disable=invalid-name
        """how much would the cluster expand by if this table were added"""
        columns_to_add = set(self.T2C[tablename])
        current_columns = set(self.all_columns)
        union = 1.0 * len(columns_to_add | current_columns) / len(current_columns) - 1
        # print(f"table: {tablename} union: {union}")
        return union

    def can_add_to_cluster(self, tablename):
        """returns whether this table can be added to the cluster"""
        # intersection musn't be too small, union musn't be too big
        return (
            self.overlap_X(tablename) > self.threshold
            and self.overlap_U(tablename) < 1 - self.threshold
        )

    def print(self):
        """prints the cluster"""
        df = pd.DataFrame({})
        df["columns"] = list(self.all_columns)
        for tablename in self.members:
            table_columns = self.T2C[tablename]
            df[tablename] = df["columns"].apply(lambda x: x in table_columns)
        print(df.T)

    def clustersize(self):
        """returns the number of members in this cluster"""
        return len(self.members)


def get_largest_cluster(
    p_t2c: dict,
    p_mergespec: dict,
):
    """determine and return the largest cluster"""
    # start with an arbitrary table
    largest_cluster = None
    for initial_table in p_mergespec["tables"]:
        c1 = Cluster(p_t2c, initial_table["tablename"], args.threshold)

        for table in p_mergespec["tables"][1:]:
            if c1.can_add_to_cluster(table["tablename"]):
                # print("adding table:", table["tablename"])
                c1.add_table(table["tablename"])

        if largest_cluster is None or c1.clustersize() > largest_cluster.clustersize():
            largest_cluster = c1

        # c1.print()

    return largest_cluster


# -- start
# dbtproject = dbtProject(project_dir)
# dbtproject.ensure_models_dir(mergespec["outputsschema"])
t2c = get_column_lists(warehouse, mergespec, connection_info, working_dir)

while len(mergespec["tables"]) > 0:
    this_cluster = get_largest_cluster(t2c, mergespec)
    print(f"cluster: {this_cluster.first_table} {this_cluster.clustersize()}")
    this_cluster.print()

    # -- remove the first cluster from the list of tables
    for table_name in this_cluster.members:
        for table in mergespec["tables"]:
            if table["tablename"] == table_name:
                mergespec["tables"].remove(table)
                break
