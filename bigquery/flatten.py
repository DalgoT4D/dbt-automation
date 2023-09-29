"""Bigquer module"""
import os
import json
import sys
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
from lib.sourceschemas import get_source
from lib.dbtproject import dbtProject
from lib.postgres import cleaned_column_name
from lib.postgres import dedup_list
from lib.dbtconfigs import mk_model_config


# from lib.postgres import get_json_columnspec

load_dotenv("bg.env")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
)

from google.cloud import bigquery

client = bigquery.Client()


basicConfig(level=INFO)
logger = getLogger()


# ================================================================================
def mk_dbtmodel(sourcename: str, srctablename: str, columntuples: list):
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

    for json_field, sql_column in columntuples:
        dbtmodel += f", _airbyte_data->>'{json_field}' as \"{sql_column}\""
        dbtmodel += "\n"

    dbtmodel += f"FROM {{{{source('{sourcename}','{srctablename}')}}}}"

    return dbtmodel


# ================================================================================
dbtproject = dbtProject(os.getenv("DBT_PROJECT_DIR"))
SOURCE_SCHEMA = "dbt_automation"
DEST_SCHEMA = "dbt_automation_int"

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

    # get the field names from the json objects
    query = client.query(
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
        sql_columns = list(map(cleaned_column_name, json_fields))

    # after cleaning we may have duplicates
    sql_columns = dedup_list(sql_columns)

    # create the configuration
    model_config = mk_model_config(DEST_SCHEMA, modelname, sql_columns)
    models.append(model_config)

    # and the .sql model
    model_sql = mk_dbtmodel(
        source["name"],  # pass the source in the yaml file
        modelname,
        zip(json_fields, sql_columns),
    )
    dbtproject.write_model(DEST_SCHEMA, modelname, model_sql, logger=logger)


# finally write the yml with the models configuration
dbtproject.write_model_config(DEST_SCHEMA, models, logger=logger)
