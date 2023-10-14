"""generates models to flatten airbyte raw data"""
import os
import json
import sys
import argparse
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv

from lib.sourceschemas import get_source
from lib.dbtproject import dbtProject
from lib.dbtconfigs import mk_model_config
from lib.postgres import get_columnspec as db_get_colspec
from lib.columnutils import make_cleaned_column_names, dedup_list
from lib.postgres import get_json_columnspec as db_get_json_colspec

basicConfig(level=INFO)
logger = getLogger()

load_dotenv("dbconnection.env")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
)

from google.cloud import bigquery

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
parser.add_argument("--source-schema", required=True)
parser.add_argument("--dest-schema", required=True)
args = parser.parse_args()

project_dir = os.getenv("DBT_PROJECT_DIR")

bg_client = bigquery.Client()

# ================================================================================
connection_info = {
    "DBHOST": os.getenv("DBHOST"),
    "DBPORT": os.getenv("DBPORT"),
    "DBUSER": os.getenv("DBUSER"),
    "DBPASSWORD": os.getenv("DBPASSWORD"),
    "DBNAME": os.getenv("DBNAME"),
}


def get_columnspec(schema: str, table: str):
    """get the column schema for this table"""
    return db_get_colspec(
        schema,
        table,
        connection_info,
    )


def get_json_columnspec(schema: str, table: str):
    """get the column schema for this table"""
    return db_get_json_colspec(
        schema,
        table,
        connection_info,
    )


# ================================================================================
def mk_dbtmodel(warehouse, sourcename: str, srctablename: str, columntuples: list):
    """create the .sql model for this table"""

    dbtmodel = """
{{ 
  config(
    materialized='table',
    indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]
  ) 
}}
    """
    dbtmodel += "SELECT _airbyte_ab_id "
    dbtmodel += "\n"

    if warehouse == "bigquery":
        for json_field, sql_column in columntuples:
            json_field = json_field.replace("'", "\\'")
            dbtmodel += (
                f", json_value(_airbyte_data, '$.\"{json_field}\"') as `{sql_column}`"
            )
            dbtmodel += "\n"

        dbtmodel += f"FROM {{{{source('{sourcename}','{srctablename}')}}}}"

    if warehouse == "postgres":
        for json_field, sql_column in columntuples:
            dbtmodel += f", _airbyte_data->>'{json_field}' as \"{sql_column}\""
            dbtmodel += "\n"

        dbtmodel += f"FROM {{{{source('{sourcename}','{srctablename}')}}}}"

    return dbtmodel


# ================================================================================
dbtproject = dbtProject(project_dir)
SOURCE_SCHEMA = args.source_schema
DEST_SCHEMA = args.dest_schema

# create the output directory
dbtproject.ensure_models_dir(DEST_SCHEMA)

# locate the sources.yml for the input-schema
sources_filename = dbtproject.sources_filename(SOURCE_SCHEMA)

# find the source in that file... it should be the only one
source = get_source(sources_filename, SOURCE_SCHEMA)
if source is None:
    logger.error("no source for schema %s in %s", SOURCE_SCHEMA, sources_filename)
    sys.exit(1)

# for every table in the source, generate an output model file

models = []

for srctable in source["tables"]:
    modelname = srctable["name"]
    tablename = srctable["identifier"]

    sql_columns = []
    json_fields = []
    if args.warehouse == "postgres":
        # get the field names from the json objects
        json_fields = get_json_columnspec(SOURCE_SCHEMA, tablename)

        # convert to sql-friendly column names
        sql_columns = make_cleaned_column_names(json_fields)

        # after cleaning we may have duplicates
        sql_columns = dedup_list(sql_columns)

    if args.warehouse == "bigquery":
        # get the field names from the json objects
        print("table", tablename)
        query = bg_client.query(
            f"""
                SELECT * FROM `{SOURCE_SCHEMA}`.`{tablename}` LIMIT 1
            """,
            location="asia-south1",
        )

        # fetch the col names
        # convert to sql-friendly column names
        sql_columns = []
        json_fields = []
        for row in query:
            json_fields = json.loads(row["_airbyte_data"]).keys()
            sql_columns = make_cleaned_column_names(json_fields)

    # create the configuration
    model_config = mk_model_config(DEST_SCHEMA, modelname, sql_columns)
    models.append(model_config)

    # and the .sql model
    model_sql = mk_dbtmodel(
        args.warehouse,
        source["name"],  # pass the source in the yaml file
        modelname,
        zip(json_fields, sql_columns),
    )
    dbtproject.write_model(DEST_SCHEMA, modelname, model_sql, logger=logger)


# finally write the yml with the models configuration
dbtproject.write_model_config(DEST_SCHEMA, models, logger=logger)
