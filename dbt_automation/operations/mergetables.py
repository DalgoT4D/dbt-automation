"""takes a list of tables and a common column spec and creates a dbt model to merge them"""

import os

import argparse
from collections import Counter
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def union_tables_sql(config, warehouse: WarehouseInterface):
    """Generates SQL code for unioning tables using the dbt_utils union_relations macro."""
    input_arr = config["input_arr"]
    dest_schema = config["dest_schema"]

    names = set()
    for input in input_arr:
        name = source_or_ref(**input)
        if name in names:
            logger.error("This appears more than once: %s", name)
            raise ValueError("Duplicate inputs found")
        names.add(name)

    relations = "["
    for input in input_arr:
        relations += f"{source_or_ref(**input)},"
    relations = relations[:-1]
    relations += "]"
    dbt_code = f"{{{{ config(materialized='table',schema='{dest_schema}') }}}}\n"
    # pylint:disable=consider-using-f-string
    dbt_code += "{{ dbt_utils.union_relations("
    dbt_code += f"relations={relations}"
    dbt_code += ")}}"

    return dbt_code


def union_tables(config, warehouse: WarehouseInterface, project_dir):
    """Generates a dbt model which uses the dbt_utils union_relations macro to union tables."""
    output_model_name = config["output_name"]

    union_code = union_tables_sql(config, warehouse)

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(config["dest_schema"])

    model_sql_path = dbtproject.write_model(
        config["dest_schema"],
        output_model_name,
        union_code,
    )

    return model_sql_path


if __name__ == "__main__":
    # pylint:disable=invalid-name
    def list_of_strings(arg: str):
        """converts a comma separated string into a list of strings"""
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
