"""syncs one schema in one database to the corresponding schema in the other database"""
import sys
import argparse
from logging import basicConfig, getLogger, INFO
import yaml

from lib.postgres import get_connection

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--delete", action="store_true")
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

        statement = f"SELECT _airbyte_data->'_id' FROM staging.{tablename}"

        cursor_ref = conn_ref.cursor()
        cursor_ref.execute(statement)
        resultset_ref = cursor_ref.fetchall()

        cursor_comp = conn_comp.cursor()
        cursor_comp.execute(statement)
        resultset_comp = cursor_comp.fetchall()

        if len(resultset_ref) != len(resultset_comp):
            print(
                f"{tablename} row counts differ: ref={len(resultset_ref)} comp={len(resultset_comp)}"
            )
            assert len(resultset_comp) > len(resultset_ref)
            for extra_id in set(resultset_comp) - set(resultset_ref):
                print(f"deleting {extra_id[0]}")
                del_statement = f"DELETE FROM staging.{tablename} WHERE _airbyte_data->'_id' = '{extra_id[0]}'"
                if args.delete:
                    cursor_comp.execute(del_statement)
        cursor_comp.close()
        cursor_ref.close()
