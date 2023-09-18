"""generates models to flatten airbyte raw data"""
import os
import sys
import argparse
from logging import basicConfig, getLogger, INFO
from pathlib import Path
import yaml
from dotenv import load_dotenv

from lib.sourceschemas import get_source
from lib.dbtproject import dbtProject
from lib.postgres import get_columnspec as db_get_colspec, cleaned_column_name
from lib.postgres import get_json_columnspec as db_get_json_colspec

basicConfig(level=INFO)
logger = getLogger()

load_dotenv("dbconnection.env")

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--project-dir", required=True)
args = parser.parse_args()


# ================================================================================
connection_info = {
    "DBHOST": os.getenv("DBHOST"),
    "DBPORT": os.getenv("DBPORT"),
    "DBUSER": os.getenv("DBUSER"),
    "DBPASSWORD": os.getenv("DBPASSWORD"),
    "DBNAME": os.getenv("DBNAME"),
}


# ================================================================================
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


# print(get_json_columnspec("staging", "_airbyte_raw_daily_issue_form"))
# sys.exit(0)


# ================================================================================
def mk_model_config(schemaname: str, modelname_: str, columnspec: list):
    """iterates through the tables in a source and creates the corresponding model
    configs. only one column is specified in the model: _airbyte_ab_id"""
    columns = [
        {
            "name": "_airbyte_ab_id",
            "description": "",
            "tests": ["unique", "not_null"],
        }
    ]
    for column in columnspec:
        columns.append(
            {
                "name": column,
                "description": "",
            }
        )
    return {
        "name": modelname_,
        "description": "",
        "+schema": schemaname,
        "columns": columns,
    }


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

    dbtmodel += f"FROM {sourcename}.{srctablename}"

    return dbtmodel


# ================================================================================
dbtproject = dbtProject(args.project_dir)
SOURCE_SCHEMA = "staging"
DEST_SCHEMA = "intermediate"

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
    json_fields = get_json_columnspec(SOURCE_SCHEMA, tablename)

    # convert to sql-friendly column names
    sql_columns = list(map(cleaned_column_name, json_fields))

    # create the configuration
    model_config = mk_model_config(DEST_SCHEMA, modelname, sql_columns)
    models.append(model_config)

    # and the .sql model
    model_sql = mk_dbtmodel(
        SOURCE_SCHEMA,
        tablename,
        zip(json_fields, sql_columns),
    )
    dbtproject.write_model(DEST_SCHEMA, modelname, model_sql, logger=logger)


# finally write the yml with the models configuration
dbtproject.write_model_config(DEST_SCHEMA, models, logger=logger)
