"""
This file runs all the operations from the yaml file
"""

import argparse
import os
import yaml
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
from operations.flattenairbyte import flatten_operation
from operations.mergetables import union_tables


load_dotenv("dbconnection.env")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
)
project_dir = os.getenv("DBT_PROJECT_DIR")

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--yamlconfig",
    required=True,
    help="Absolute path of the yaml config file that defines your operations",
)
args = parser.parse_args()

# Load the YAML file
config_data = None
with open(args.yamlconfig, "r", encoding="utf-8") as yaml_file:
    config_data = yaml.safe_load(yaml_file)

if config_data is None:
    raise Exception("Couldn't read the yaml config data")

# TODO: Add stronger validations for each operation here
if config_data["warehouse"] not in ["postgres", "bigquery"]:
    raise ValueError("unknown warehouse")
warehouse = config_data["warehouse"]

# run operations to generate dbt model(s)
for op_data in config_data["operations"]:
    op_type = op_data["type"]
    config = op_data["config"]
    if op_type == "flatten":
        logger.info("running the flatten operation")
        logger.info(f"using config {config}")
        flatten_operation(config=config, warehouse=warehouse, project_dir=project_dir)
        logger.info("finished running the flatten operation")

    if op_type == "unionall":
        logger.info("running the union operation on all tables")
        logger.info(f"using config {config}")
        union_tables(config=config, warehouse=warehouse, project_dir=project_dir)
        logger.info("finished running the union all operation")
