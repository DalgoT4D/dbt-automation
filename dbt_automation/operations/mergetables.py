"""takes a list of tables and a common column spec and creates a dbt model to merge them"""
import os
import sys

import argparse
from collections import Counter
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv

from dbt_automation.utils.dbtproject import dbtProject


basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument
def union_tables(config, warehouse, project_dir):
    """generates a dbt model which uses the dbt_utils union_relations macro to union tables"""
    tablenames = config["tablenames"]
    dest_schema = config["dest_schema"]
    output_model_name = config["output_name"]

    table_counts = Counter([tablename for tablename in tablenames])
    # pylint:disable=invalid-name
    has_error = False
    for tablename, tablenamecount in table_counts.items():
        if tablenamecount > 1:
            logger.error("table appears more than once in spec: %s", tablename)
            has_error = True
    if has_error:
        raise ValueError("duplicate table names found")
    logger.info("no duplicate table names found")

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    logger.info("created the dbt project object")

    # pylint:disable=invalid-name
    logger.info(f"need to process the following tables: {tablenames}")
    relations = "["
    for tablename in tablenames:
        relations += f"ref('{tablename}'),"
    relations = relations[:-1]
    relations += "]"
    union_code = "{{ config(materialized='table',) }}\n"
    # pylint:disable=consider-using-f-string
    union_code += "{{ dbt_utils.union_relations("
    union_code += f"relations={relations}"
    union_code += ")}}"

    logger.info(f"writing dbt model {union_code}")
    dbtproject.write_model(
        dest_schema,
        output_model_name,
        union_code,
    )
    logger.info(f"dbt model {output_model_name} successfully created")


if __name__ == "__main__":
    from dotenv import load_dotenv

    def list_of_strings(arg):
        return arg.split(",")

    parser = argparse.ArgumentParser()
    parser.add_argument("--output-name", required=True)
    parser.add_argument("--dest-schema", required=True)
    parser.add_argument("--tablenames", required=True, type=list_of_strings)
    args = parser.parse_args()

    load_dotenv("dbconnection.env")
    projectdir = os.getenv("DBT_PROJECT_DIR")

    union_tables(
        config={
            "output_name": args.output_name,
            "dest_schema": args.dest_schema,
            "tablenames": args.tablenames,
        },
        warehouse="",
        project_dir=projectdir,
    )
