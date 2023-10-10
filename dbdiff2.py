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


def db2csv(
    connection,
    schema: str,
    table: str,
    columns: list,
    exclude_columns: list,
    csvfile: str,
):
    """dumps the table into a csv file"""
    columns_list = ",".join([x for x in columns if x not in exclude_columns])
    if not os.path.exists(csvfile):
        subprocess_cmd = [
            "psql",
            "-h",
            connection["host"],
            "-d",
            connection["name"],
            "-U",
            connection["user"],
            "-c",
            f"\\COPY (SELECT {columns_list} FROM {schema}.{table}) TO '{csvfile}' WITH CSV HEADER",
        ]
        os.environ["PGPASSWORD"] = connection["password"]
        subprocess.check_call(subprocess_cmd)

    sorted_csv = csvfile.replace(".csv", ".sorted.csv")
    if not os.path.exists(sorted_csv):
        subprocess_cmd = ["sort", csvfile, "-o", sorted_csv]
        subprocess.check_call(subprocess_cmd)
    return sorted_csv


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
        print(ref_tables)
        print(comp_tables)
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
    sorted_ref_csv = db2csv(
        connection_info["ref"],
        ref_schema,
        tablename,
        columns_specs[tablename],
        ["_airbyte_ab_id"],
        ref_csvfile,
    )

    # ok, now do the same for the comp
    comp_csvfile = f"{working_dir}/{comp_schema}/{tablename}.comp.csv"
    sorted_comp_csv = db2csv(
        connection_info["comp"],
        comp_schema,
        tablename,
        columns_specs[tablename],
        ["_airbyte_ab_id"],
        comp_csvfile,
    )

    # and finally compare the two sorted csv files
    with open(sorted_comp_csv, "r", encoding="utf-8") as sorted_comp, open(
        sorted_ref_csv,
        "r",
        encoding="utf-8",
    ) as sorted_ref:
        for idx, (line1, line2) in enumerate(zip(sorted_comp, sorted_ref)):
            if line1 != line2:
                print(f"mismatch for {tablename} at {idx}")
                print(line1)
                print(line2)
                sys.exit(1)

    # delete the sorted csv files
    os.remove(sorted_ref_csv)
    os.remove(sorted_comp_csv)
