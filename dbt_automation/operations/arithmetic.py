"""
This file contains the airthmetic operations for dbt automation
"""

from logging import basicConfig, getLogger, INFO
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def arithmetic_dbt_sql(config: dict):
    """
    performs arithmetic operations: +/-/*//
    config["input"] is dict {"source_name": "", "input_name": "", "input_type": ""}
    """
    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    operator = config["operator"]
    operands = config["operands"]
    output_col_name = config["output_column_name"]

    if operator not in ["add", "sub", "mul", "div"]:
        raise ValueError("Unknown operation")

    if len(operands) < 2:
        raise ValueError("At least two operands are required to perform operations")

    if operator == "div" and len(operands) != 2:
        raise ValueError("Division requires exactly two operands")

    # SQL generation logic
    dbt_code = ""

    if config["input"]["input_type"] != "cte":
        dbt_code += f"{{{{ config(materialized='table',schema='{dest_schema}') }}}}\n"

    dbt_code += "SELECT *, "

    if operator == "add":
        dbt_code += "SELECT *,"
        dbt_code += "{{dbt_utils.safe_add(["
        for operand in operands:
            dbt_code += f"'{str(operand)}',"
        dbt_code = dbt_code[:-1]
        dbt_code += "])}}"
        dbt_code += f" AS {output_col_name} "

    if operator == "mul":
        dbt_code += "SELECT *,"
        for operand in operands:
            dbt_code += f"{operand} * "
        dbt_code = dbt_code[:-2]
        dbt_code += f" AS {output_col_name} "

    if operator == "sub":
        dbt_code += "SELECT *,"
        dbt_code += "{{dbt_utils.safe_subtract(["
        for operand in operands:
            dbt_code += f"'{str(operand)}',"
        dbt_code = dbt_code[:-1]
        dbt_code += "])}}"
        dbt_code += f" AS {output_col_name} "

    if operator == "div":
        dbt_code += "SELECT *,"
        dbt_code += "{{dbt_utils.safe_divide("
        for operand in operands:
            dbt_code += f"'{str(operand)}',"
        dbt_code += ")}}"
        dbt_code += f" AS {output_col_name} "

    dbt_code += f"\nFROM {{ref('{config['input']['source_name']}.{config['input']['input_name']}')}}\n"

    return dbt_code


def arithmetic(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform arithmetic operations and generate a DBT model.
    """
    sql = arithmetic_dbt_sql(config)

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(config["dest_schema"])

    model_sql_path = dbtproject.write_model(
        config["dest_schema"], config["output_name"], sql
    )

    return model_sql_path
