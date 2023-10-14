"""helpers for postgres"""
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
