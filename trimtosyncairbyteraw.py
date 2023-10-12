"""syncs one schema in one database to the corresponding schema in the other database"""
import sys
import argparse
from logging import basicConfig, getLogger, INFO
import yaml
import json

from lib.postgres import get_connection

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--id-column", required=True)
parser.add_argument("--delete", action="store_true")
args = parser.parse_args()

with open("connections.yaml", "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

ID_COLUMN = args.id_column


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
    ref_tables = get_tables(conn_ref, "staging")
    comp_tables = get_tables(conn_comp, "staging")

    if set(ref_tables) != set(comp_tables):
        print("not the same table sets")
        print(f"ref - comp: {set(ref_tables) - set(comp_tables)}")
        print(f"comp - ref: {set(comp_tables) - set(ref_tables)}")
        sys.exit(1)

    for tablename in ref_tables:
        print(tablename)
        if not tablename.startswith("_airbyte_raw_"):
            print("skipping")
            continue

        statement = f"SELECT _airbyte_data->'{ID_COLUMN}' FROM staging.{tablename}"

        cursor_ref = conn_ref.cursor()
        cursor_ref.execute(statement)
        resultset_ref = [x[0] for x in cursor_ref.fetchall()]

        cursor_comp = conn_comp.cursor()
        cursor_comp.execute(statement)
        resultset_comp = [x[0] for x in cursor_comp.fetchall()]

        if len(resultset_ref) != len(resultset_comp):
            print(
                f"{tablename} row counts differ: ref={len(resultset_ref)} comp={len(resultset_comp)}"
            )
            resultset_comp = set(resultset_comp)
            resultset_ref = set(resultset_ref)

            if len(resultset_ref - resultset_comp) > 0:
                for extra_id in resultset_ref - resultset_comp:
                    if extra_id:
                        extra_id_statement = f"SELECT _airbyte_data FROM staging.{tablename} WHERE _airbyte_data->>'{ID_COLUMN}' = '{extra_id}'"
                        cursor_ref.execute(extra_id_statement)
                        extra_id_result = cursor_ref.fetchone()
                        airbyte_data = extra_id_result[0]
                        print(json.dumps(airbyte_data, indent=2))
                        print("=" * 80)
                    else:
                        print(f"found _airbyte_data->>{ID_COLUMN} = None in ref")
                sys.exit(1)

            for extra_id in resultset_comp - resultset_ref:
                if extra_id:
                    print(f"deleting {extra_id[0]}")
                    del_statement = f"DELETE FROM staging.{tablename} WHERE _airbyte_data->>'{ID_COLUMN}' = '{extra_id[0]}'"
                    if args.delete:
                        cursor_comp.execute(del_statement)
                else:
                    print(f"found _airbyte_data->>{ID_COLUMN} = None in comp")

        cursor_comp.close()
        cursor_ref.close()
