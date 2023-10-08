"""diffs two schemas in two databases"""
import sys
import argparse
from logging import basicConfig, getLogger, INFO
import yaml
from tqdm import tqdm

from lib.postgres import get_connection

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--ref-schema", required=True)
parser.add_argument("--comp-schema", required=True)
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


def get_row_count(connection, schema: str, table_name: str) -> int:
    """returns the number of rows in the table"""
    cursor = connection.cursor()
    cursor.execute(
        f"""
          SELECT COUNT(1) FROM {schema}.{table_name}
        """
    )
    nrows = cursor.fetchone()[0]
    cursor.close()
    return nrows


def make_check_statement(
    schema: str, table_name: str, columns: list, values: list
) -> str:
    """create a SELECT query which matches the columns against the given values"""
    filters = []

    skip_columns = ["_airbyte_ab_id"]

    for colname, colvalue in zip(columns, values):
        if colname in skip_columns:
            continue
        if colvalue is None:
            filters.append(f'"{colname}" IS NULL')
        elif colvalue.find(chr(39)) < 0:
            filters.append(f"\"{colname}\" = '{colvalue}'")

    check_statement = f"SELECT COUNT(1) FROM {schema}.{table_name} WHERE "
    check_statement += " AND ".join(filters)

    return check_statement


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

    for tablename in ref_tables:
        print(tablename)
        col_schema_ref = get_columns_spec(conn_ref, ref_schema, tablename)
        col_schema_comp = get_columns_spec(
            conn_comp,
            comp_schema,
            tablename,
        )
        if set(col_schema_ref) != set(col_schema_comp):
            print(f"column schemas differ for {tablename}")
            sys.exit(1)

        rows_ref = get_row_count(conn_ref, ref_schema, tablename)
        rows_comp = get_row_count(conn_comp, comp_schema, tablename)
        if rows_ref != rows_comp:
            print(f"{tablename} row counts differ: ref={rows_ref} comp={rows_comp}")
            # sys.exit(1)

        # continue
        # now check the rows
        quoted_column_names = [f'"{c}"' for c in col_schema_ref]
        columnlist = ", ".join(quoted_column_names)
        statement = f"SELECT {columnlist} FROM {ref_schema}.{tablename}"

        cursor_ref = conn_ref.cursor()
        cursor_ref.execute(statement)
        resultset = cursor_ref.fetchall()

        cursor_comp = conn_comp.cursor()

        for row in tqdm(resultset, desc=f"Checking {tablename}..."):
            check_statement = make_check_statement(
                comp_schema, tablename, col_schema_ref, row
            )
            cursor_comp.execute(check_statement)
            nmatches = cursor_comp.fetchone()[0]
            if nmatches != 1:
                print(f"mismatch for {tablename} and query {check_statement}")
                sys.exit(1)

        cursor_comp.close()
        cursor_ref.close()
