"""reads tables from db to create a dbt sources.yml"""
import os
import argparse
from logging import basicConfig, getLogger, INFO
from pathlib import Path
import yaml

from dotenv import load_dotenv

load_dotenv("dbconnection.env")

# pylint:disable=wrong-import-position
from dbt_automation.utils.sourceschemas import mksourcedefinition
from dbt_automation.utils.warehouseclient import get_client
from dbt_automation.utils.dbtsources import (
    readsourcedefinitions,
    merge_sourcedefinitions,
)

basicConfig(level=INFO)
logger = getLogger()


def sync_sources(config, warehouse, project_dir):
    """
    reads tables from the input_schema to create a dbt sources.yml
    uses the metadata from the existing source definitions, if any
    """
    input_schema = config["source_schema"]
    source_name = config["source_name"]
    source_dir = Path(project_dir) / "models" / input_schema
    sources_file = Path(project_dir) / "models" / input_schema / "sources.yml"

    tablenames = warehouse.get_tables(input_schema)
    dbsourcedefinitions = mksourcedefinition(source_name, input_schema, tablenames)
    logger.info("read sources from database schema %s", input_schema)

    if source_dir.exists() is False:
        os.mkdir(source_dir)

    if sources_file.exists():
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

    warehouse.close()


if __name__ == "__main__":
    load_dotenv("dbconnection.env")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    projectdir = os.getenv("DBT_PROJECT_DIR")

    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", required=True, choices=["postgres", "bigquery"])
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--source-name", required=True)
    args = parser.parse_args()

    conn_info = {
        "host": os.getenv("DBHOST"),
        "port": os.getenv("DBPORT"),
        "username": os.getenv("DBUSER"),
        "password": os.getenv("DBPASSWORD"),
        "database": os.getenv("DBNAME"),
    }
    warehouse_client = get_client(args.warehouse, conn_info)

    sync_sources(
        config={"source_schema": args.source_schema, "source_name": "Sheets"},
        warehouse=warehouse_client,
        project_dir=projectdir,
    )
