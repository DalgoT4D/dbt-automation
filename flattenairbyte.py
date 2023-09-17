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
def get_model_config(modelname: str, schemaname: str, columnspec: list):
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
                "name": cleaned_column_name(column),
                "description": "",
            }
        )
    return {
        "name": modelname,
        "description": "",
        "+schema": schemaname,
        "columns": columns,
    }


# ================================================================================
def mk_dbtmodel(sourcename: str, srctablename: str, columns: list):
    """create the .sql model for this table"""

    assert len(columns) > 0

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

    for colname in columns:
        dbtmodel += (
            f", _airbyte_data->>'{colname}' as \"{cleaned_column_name(colname)}\""
        )
        dbtmodel += "\n"

    dbtmodel += f"FROM {sourcename}.{srctablename}"

    return dbtmodel


# ================================================================================
dbtproject = dbtProject(args.project_dir)

# create the output directory
dbtproject.ensure_models_dir("intermediate")

# locate the sources.yml for the input-schema
sources_filename = dbtproject.sources_filename("staging")

# find the source in that file... it should be the only one
source = get_source(sources_filename, "staging")
if source is None:
    logger.error("no source for schema %s in %s", "staging", sources_filename)
    sys.exit(1)

# for every table in the source, generate an output model file
models_dir = dbtproject.models_dir("intermediate")
models = []
for srctable in source["tables"]:
    colspec = get_json_columnspec(source["schema"], srctable["identifier"])

    models.append(get_model_config(srctable["name"], "intermediate", colspec))

    model_filename = Path(models_dir) / (srctable["name"] + ".sql")
    logger.info(
        "[write_models] %s.%s => %s", source["name"], srctable["name"], model_filename
    )

    with open(model_filename, "w", encoding="utf-8") as outfile:
        model = mk_dbtmodel(source["schema"], srctable["identifier"], colspec)
        outfile.write(model)
        outfile.close()

# write the yml with the models configuration
models_filename = dbtproject.models_filename("intermediate")
with open(models_filename, "w", encoding="utf-8") as models_file:
    logger.info("writing %s", models_filename)
    yaml.safe_dump(
        {
            "version": 2,
            "models": models,
        },
        models_file,
        sort_keys=False,
    )
