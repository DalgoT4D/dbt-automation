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

working_dir = args.working_dir


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
    columns_list = [f'"{x}"' for x in columns if x not in exclude_columns]
    if not os.path.exists(csvfile):
        cmdfile = f"{working_dir}/{table}.cmd"
        print(f"writing {cmdfile}")
        with open(cmdfile, "w", encoding="utf-8") as cmd:
            cmd.write("\\COPY (SELECT ")
            cmd.write(",".join(columns_list))
            cmd.write(f" FROM {schema}.{table}) TO '{csvfile}' WITH CSV HEADER")
            cmd.close()
        subprocess_cmd = [
            "psql",
            "-h",
            connection["host"],
            "-p",
            connection.get("port", "5432"),
            "-d",
            connection["name"],
            "-U",
            connection["user"],
            "-f",
            cmdfile,
        ]
        os.environ["PGPASSWORD"] = connection["password"]
        subprocess.check_call(subprocess_cmd)
        os.unlink(cmdfile)

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
        print("WARNING: not the same table sets")
        if len(ref_tables) > len(comp_tables):
            print("ref has more tables")
            print(set(ref_tables) - set(comp_tables))
        else:
            print("comp has more tables")
            print(set(comp_tables) - set(ref_tables))

        ref_tables = set(ref_tables) & set(comp_tables)

    columns_specs = {}
    for tablename in ref_tables:
        ref_columns = get_columns_spec(conn_ref, ref_schema, tablename)
        comp_columns = get_columns_spec(conn_comp, comp_schema, tablename)
        if set(ref_columns) != set(comp_columns):
            print(f"columns for {tablename} are not the same")
            if len(ref_columns) > len(comp_columns):
                print("ref has more columns")
                print(set(ref_columns) - set(comp_columns))
            else:
                print("comp has more columns")
                print(set(comp_columns) - set(ref_columns))
            sys.exit(1)
        columns_specs[tablename] = ref_columns

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
    print(f"TABLE: {tablename}")

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

    has_mismatches = False
    # and finally compare the two sorted csv files
    with open(sorted_comp_csv, "r", encoding="utf-8") as sorted_comp, open(
        sorted_ref_csv,
        "r",
        encoding="utf-8",
    ) as sorted_ref:
        for idx, (line1, line2) in enumerate(zip(sorted_comp, sorted_ref)):
            if line1 != line2:
                print(f"mismatch for {tablename} at {idx}")
                print(line1.strip())
                print(line2.strip())
                has_mismatches = True

    if not has_mismatches:
        # delete the sorted csv files
        os.remove(sorted_ref_csv)
        os.remove(sorted_comp_csv)
