"""
This file runs all the operations from the yaml file
"""

import argparse
import os
from logging import basicConfig, getLogger, INFO
import yaml
from dotenv import load_dotenv
from operations.flattenairbyte import flatten_operation
from operations.mergetables import union_tables
from operations.syncsources import sync_sources
from operations.castdatatypes import cast_datatypes
from operations.arithmetic import arithmetic

OPERATIONS_DICT = {
    "flatten": flatten_operation,
    "unionall": union_tables,
    "syncsources": sync_sources,
    "castdatatypes": cast_datatypes,
    "arithmetic": arithmetic,
}

load_dotenv("dbconnection.env")

project_dir = os.getenv("DBT_PROJECT_DIR")

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser(
    description="Run operations from the yaml config file: "
    + ", ".join(OPERATIONS_DICT.keys())
)
parser.add_argument(
    "-y",
    "--yamlconfig",
    required=True,
    help="Path to the yaml config file that defines your operations",
)
args = parser.parse_args()

# Load the YAML file
config_data = None
with open(args.yamlconfig, "r", encoding="utf-8") as yaml_file:
    config_data = yaml.safe_load(yaml_file)

if config_data is None:
    raise ValueError("Couldn't read the yaml config data")

# TODO: Add stronger validations for each operation here
if config_data["warehouse"] not in ["postgres", "bigquery"]:
    raise ValueError("unknown warehouse")
warehouse = config_data["warehouse"]

# run operations to generate dbt model(s)
for op_data in config_data["operations"]:
    op_type = op_data["type"]
    config = op_data["config"]

    if op_type not in OPERATIONS_DICT:
        raise ValueError("unknown operation")

    logger.info(f"running the {op_type} operation")
    logger.info(f"using config {config}")
    OPERATIONS_DICT[op_type](
        config=config, warehouse=warehouse, project_dir=project_dir
    )
    logger.info(f"finished running the {op_type} operation")
