"""helpers for postgres"""
import re
from collections import Counter
import psycopg2


# ================================================================================
def get_connection(host: str, port: str, user: str, password: str, database: str):
    """returns a psycopg connection"""
    connection = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    return connection


def get_columnspec(schema: str, table: str, conn_info: dict):
    """get the column schema for this table"""
    with get_connection(
        conn_info["DBHOST"],
        conn_info["DBPORT"],
        conn_info["DBUSER"],
        conn_info["DBPASSWORD"],
        conn_info["DBNAME"],
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
            """
        )
        return [x[0] for x in cursor.fetchall()]


def get_json_columnspec(schema: str, table: str, conn_info: dict):
    """get the column schema from the _airbyte_data json field for this table"""
    with get_connection(
        conn_info["DBHOST"],
        conn_info["DBPORT"],
        conn_info["DBUSER"],
        conn_info["DBPASSWORD"],
        conn_info["DBNAME"],
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT DISTINCT 
                    jsonb_object_keys(_airbyte_data)
                FROM {schema}.{table}
            """
        )
        return [x[0] for x in cursor.fetchall()]


def cleaned_column_name(colname: str) -> str:
    """cleans the column name"""
    colname = colname[:40]
    pattern = r"[^0-9a-zA-Z]"
    colname = re.sub(pattern, "_", colname)
    if colname.isdigit():
        colname = "c" + colname
    return colname


def make_cleaned_column_names(columns: list) -> list:
    """cleans the column names"""
    cleaned_names = [cleaned_column_name(colname) for colname in columns]
    if len(set(cleaned_names)) != len(cleaned_names):
        cleaned_names = dedup_list(cleaned_names)
    return cleaned_names


def dedup_list(names: list):
    """ensures list does not contain duplicates, by appending to
    any duplicates found"""
    column_name_counts = Counter()
    deduped_names = []
    for colname in names:
        column_name_counts[colname] += 1
        if column_name_counts[colname] == 1:
            deduped_name = colname
        else:
            deduped_name = (
                colname + "_" + chr(column_name_counts[colname] - 1 + ord("a"))
            )
        deduped_names.append(deduped_name)
    return deduped_names


def fmt_colname(colname: str, warehouse: str):
    """format a column name for the target warehouse"""
    if warehouse == "postgres":
        return '"' + colname + '"'
    elif warehouse == "bigquery":
        return colname.lower()
    else:
        raise ValueError(f"unsupported warehouse: {warehouse}")
