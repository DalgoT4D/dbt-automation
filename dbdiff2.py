"""diffs two schemas in two databases"""
import os
import sys
import argparse
from logging import basicConfig, getLogger, INFO
import subprocess
import yaml

from lib.postgres import get_connection

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--ref-schema", required=True)
parser.add_argument("--comp-schema", required=True)
parser.add_argument("--working-dir", required=True)
args = parser.parse_args()

with open("connections.yaml", "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)


def get_tables(connection, schema: str):
    """returns the list of table names in the given schema"""
    cursor = connection.cursor()
    cursor.execute(
        f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}'
    """
    )
    tablenames = [x[0] for x in cursor.fetchall()]
    cursor.close()
    return tablenames


def get_columns_spec(conn, schema: str, table: str):
    """returns the list of columns"""
    cursor = conn.cursor()
    cursor.execute(
        f"""SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table}'
        """
    )
    return [x[0] for x in cursor.fetchall()]


ref_schema = args.ref_schema
comp_schema = args.comp_schema

with get_connection(
    connection_info["ref"]["host"],
    connection_info["ref"]["port"],
    connection_info["ref"]["user"],
    connection_info["ref"]["password"],
    connection_info["ref"]["name"],
) as conn_ref, get_connection(
    connection_info["comp"]["host"],
    connection_info["comp"]["port"],
    connection_info["comp"]["user"],
    connection_info["comp"]["password"],
    connection_info["comp"]["name"],
) as conn_comp:
    ref_tables = get_tables(conn_ref, ref_schema)
    comp_tables = get_tables(conn_comp, comp_schema)

    if set(ref_tables) != set(comp_tables):
        print("not the same table sets")
        sys.exit(1)

    columns_specs = {}
    for tablename in ref_tables:
        ref_columns = get_columns_spec(conn_ref, ref_schema, tablename)
        comp_columns = get_columns_spec(conn_comp, comp_schema, tablename)
        if set(ref_columns) != set(comp_columns):
            print(f"columns for {tablename} are not the same")
            sys.exit(1)
        columns_specs[tablename] = ref_columns

working_dir = args.working_dir
os.makedirs(working_dir, exist_ok=True)
os.makedirs(f"{working_dir}/{ref_schema}", exist_ok=True)
os.makedirs(f"{working_dir}/{comp_schema}", exist_ok=True)

ref_host = connection_info["ref"]["host"]
ref_database = connection_info["ref"]["name"]
ref_user = connection_info["ref"]["user"]

comp_host = connection_info["comp"]["host"]
comp_database = connection_info["comp"]["name"]
comp_user = connection_info["comp"]["user"]

for tablename in ref_tables:
    print(tablename)
    ref_csvfile = f"{working_dir}/{ref_schema}/{tablename}.ref.csv"
    columns_list = ",".join(columns_specs[tablename])
    subprocess_cmd = [
        "psql",
        "-h",
        ref_host,
        "-d",
        ref_database,
        "-U",
        ref_user,
        "-c",
        f"\\COPY (SELECT {columns_list} FROM {ref_schema}.{tablename}) TO '{ref_csvfile}' WITH CSV HEADER",
    ]
    os.environ["PGPASSWORD"] = connection_info["ref"]["password"]
    subprocess.check_call(subprocess_cmd)

    # now drop the _airbyte_ab_id column
    dropped_csv = f"{working_dir}/{ref_schema}/{tablename}.ref.dropped.csv"
    subprocess_cmd = [
        "python",
        "dropcolumnfromcsv.py",
        ref_csvfile,
        dropped_csv,
        "_airbyte_ab_id",
    ]
    subprocess.check_call(subprocess_cmd)

    # now sort the dropped csv
    sorted_csv = f"{working_dir}/{ref_schema}/{tablename}.ref.sorted.csv"
    subprocess_cmd = ["sort", dropped_csv, "-o", sorted_csv]
    subprocess.check_call(subprocess_cmd)

    # ok, now do the same for the comp
    comp_csvfile = f"{working_dir}/{comp_schema}/{tablename}.comp.csv"
    subprocess_cmd = [
        "psql",
        "-h",
        comp_host,
        "-d",
        comp_database,
        "-U",
        comp_user,
        "-c",
        f"\\COPY (SELECT {columns_list} FROM {comp_schema}.{tablename}) TO '{comp_csvfile}' WITH CSV HEADER",
    ]
    os.environ["PGPASSWORD"] = connection_info["comp"]["password"]
    subprocess.check_call(subprocess_cmd)

    # now drop the _airbyte_ab_id column
    dropped_csv = f"{working_dir}/{comp_schema}/{tablename}.comp.dropped.csv"
    subprocess_cmd = [
        "python",
        "dropcolumnfromcsv.py",
        comp_csvfile,
        dropped_csv,
        "_airbyte_ab_id",
    ]
    subprocess.check_call(subprocess_cmd)

    # now sort the dropped csv
    sorted_csv = f"{working_dir}/{comp_schema}/{tablename}.comp.sorted.csv"
    subprocess_cmd = ["sort", dropped_csv, "-o", sorted_csv]
    subprocess.check_call(subprocess_cmd)

    # and finally compare the two sorted csv files
    subprocess_cmd = [
        "diff",
        sorted_csv,
        f"{working_dir}/{ref_schema}/{tablename}.ref.sorted.csv",
    ]
    try:
        subprocess.check_call(subprocess_cmd)
    except subprocess.CalledProcessError:
        print(f"diff failed for {tablename}")
        sys.exit(1)

    break
