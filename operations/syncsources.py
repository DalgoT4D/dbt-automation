"""reads tables from db to create a dbt sources.yml"""
import os
import argparse
from logging import basicConfig, getLogger, INFO
from pathlib import Path
import yaml

from dotenv import load_dotenv

load_dotenv("dbconnection.env")

# pylint:disable=wrong-import-position
from lib.sourceschemas import mksourcedefinition
from lib.postgres import PostgresClient
from lib.bigquery import BigQueryClient
from lib.dbtsources import (
    readsourcedefinitions,
    merge_sourcedefinitions,
)

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
#     os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
# )


basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser(
    """
Generates a sources.yml configuration containing exactly one source
That source will have one or more tables
Ref: https://docs.getdbt.com/reference/source-properties
Database connection parameters are read from syncsources.env
"""
)
parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
parser.add_argument("--source-name", required=True)
parser.add_argument("--schema", default="staging", help="e.g. staging")
args = parser.parse_args()

project_dir = os.getenv("DBT_PROJECT_DIR")


# ================================================================================
def make_source_definitions(
    warehouse: str, source_name: str, input_schema: str, sources_file: str
):
    """
    reads tables from the input_schema to create a dbt sources.yml
    uses the metadata from the existing source definitions, if any
    """
    if warehouse == "postgres":
        client = PostgresClient()

    elif warehouse == "bigquery":
        client = BigQueryClient()

    tablenames = client.get_tables(input_schema)
    dbsourcedefinitions = mksourcedefinition(source_name, input_schema, tablenames)
    logger.info("read sources from database schema %s", input_schema)

    if Path(sources_file).exists():
        filesourcedefinitions = readsourcedefinitions(sources_file)
        logger.info("read existing source defs from %s", sources_file)

    else:
        filesourcedefinitions = {"version": 2, "sources": []}

    merged_definitions = merge_sourcedefinitions(
        filesourcedefinitions, dbsourcedefinitions
    )
    logger.info("created (new) source definitions")
    with open(sources_file, "w", encoding="utf-8") as outfile:
        yaml.safe_dump(merged_definitions, outfile, sort_keys=False)
        logger.info("wrote source definitions to %s", sources_file)


# ================================================================================
if __name__ == "__main__":
    sources_filename = Path(project_dir) / "models" / args.schema / "sources.yml"
    make_source_definitions(
        args.warehouse, args.source_name, args.schema, sources_filename
    )
