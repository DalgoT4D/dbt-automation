"""
This file contains the airthmetic operations for dbt automation
"""

from logging import basicConfig, getLogger, INFO
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.columnutils import quote_columnname, quote_constvalue

from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


def generic_function_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    model_name: name of the output model
    function_name: name of the SQL function
    operands: list of operands (column names or constant values)
    output_name: name of the output column
    """
    function_name = config["function_name"]
    operands = config["operands"]
    source_columns = config["source_columns"]
    output_name = config["output_name"]

    if source_columns == "*":
        dbt_code = "SELECT *\n"
    else:
        dbt_code = f"SELECT {', '.join([quote_columnname(col, warehouse.name) for col in source_columns])}\n"

    dbt_code = f"SELECT {function_name}({', '.join([str(operand) for operand in operands])}) AS {output_name}, {', '.join(source_columns)}"

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += f" FROM {select_from}\n"
    else:
        dbt_code += f" FROM {{{{{select_from}}}}}\n"

    return dbt_code


def generic_function(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform a generic SQL function operation.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement = generic_function_dbt_sql(config, warehouse)

    dest_schema = config["dest_schema"]
    output_name = config["output_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    model_sql_path = dbtproject.write_model(dest_schema, output_name, dbt_sql + select_statement)

    return model_sql_path