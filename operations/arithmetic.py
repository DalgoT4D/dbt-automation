"""
This file contains the airthmetic operations for dbt automation
"""


from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
from lib.dbtproject import dbtProject

basicConfig(level=INFO)
logger = getLogger()


def arithmetic(config: dict, warehouse: str, project_dir: str):
    output_name = config["output_name"]
    input_model = config["input_model"]
    dest_schema = config["dest_schema"]
    operator = config["operator"]
    operands = config["operands"]

    if operator not in ["add", "sub", "mul", "div"]:
        raise ValueError("unknown operation")

    if len(operands) <= 1:
        raise ValueError("need atleast two operands to perform operations")

    if operator == "div" and (len(operands) != 2):
        raise ValueError("division needs exactly two operands")

    # setup the dbt project
    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    dbt_code = "{{ config(materialized='table',schema='" + dest_schema + "') }}\n"

    if operator == "add":
        dbt_code += "SELECT *,"
        for operand in operands:
            dbt_code += f"{operand} + "
        dbt_code = dbt_code[:-2]
        dbt_code += f" AS {output_name} "

    if operator == "mul":
        dbt_code += "SELECT *,"
        for operand in operands:
            dbt_code += f"{operand} * "
        dbt_code = dbt_code[:-2]
        dbt_code += f" AS {output_name} "

    if operator == "sub":
        dbt_code += "SELECT *,"
        for operand in operands:
            dbt_code += f"{operand} - "
        dbt_code = dbt_code[:-2]
        dbt_code += f" AS {output_name} "

    if operator == "div":
        dbt_code += "SELECT *,"
        dbt_code += "{{dbt_utils.safe_divide("
        for operand in operands:
            dbt_code += f"'{str(operand)}',"
        dbt_code += ")}}"
        dbt_code += f" AS {output_name} "

    dbt_code += " FROM " + "{{ref('" + input_model + "')}}" + "\n"

    logger.info(f"writing dbt model {dbt_code}")
    dbtproject.write_model(dest_schema, output_name, dbt_code)
    logger.info(f"dbt model {output_name} successfully created")
