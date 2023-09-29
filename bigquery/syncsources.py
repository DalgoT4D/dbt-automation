"""Bigquer module"""
import yaml
import os
from pathlib import Path
import sys
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
from lib.sourceschemas import get_source
from lib.dbtproject import dbtProject
from lib.sourceschemas import mksourcedefinition
from lib.dbtsources import (
    readsourcedefinitions,
    merge_sourcedefinitions,
    mergesource,
    mergetable,
)

# from lib.postgres import get_json_columnspec

load_dotenv("bg.env")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
)

from google.cloud import bigquery

client = bigquery.Client()


project_dir = os.getenv("DBT_PROJECT_DIR")
DATASET = "dbt_automation"
SOURCE_SCHEMA = "staging"

basicConfig(level=INFO)
logger = getLogger()

# get all the table names
tables = client.list_tables(DATASET)

tablenames = [x.table_id for x in tables]
dbsourcedefinitions = mksourcedefinition("sheets", "dbt_automation", tablenames)

sources_filename = Path(project_dir) / "models" / SOURCE_SCHEMA / "sources.yml"

if Path(sources_filename).exists():
    filesourcedefinitions = readsourcedefinitions(sources_filename)
    logger.info("read existing source defs from %s", sources_filename)
else:
    filesourcedefinitions = {"version": 2, "sources": []}

merged_definitions = merge_sourcedefinitions(filesourcedefinitions, dbsourcedefinitions)
logger.info("created (new) source definitions")
with open(sources_filename, "w", encoding="utf-8") as outfile:
    yaml.safe_dump(merged_definitions, outfile, sort_keys=False)
    logger.info("wrote source definitions to %s", sources_filename)
