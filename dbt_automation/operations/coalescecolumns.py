"""generates a model which coalesces columns"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def coalesce_columns_dbt_sql(config: dict, warehouse: WarehouseInterface):
    """
    Generate SQL code for the coalesce_columns operation.
    """

    dest_schema = config["dest_schema"]

    columns = config["columns"]
    columnnames = [c["columnname"] for c in columns]
    output_name = config["output_name"]

    dbt_code = ""

    if config["input"]["input_type"] != "cte":
        dbt_code += f"{{{{ config(materialized='table',schema='{dest_schema}') }}}}\n"

    dbt_code += (
        "SELECT {{dbt_utils.star(from="
        + source_or_ref(**config["input"])
        + ", except=["
    )
    dbt_code += ",".join([f'"{columnname}"' for columnname in columnnames])
    dbt_code += "])}}"

    dbt_code += ", COALESCE("

    for column in config["columns"]:
        dbt_code += quote_columnname(column["columnname"], warehouse.name) + ", "
    dbt_code = dbt_code[:-2] + ") AS " + config["output_column_name"]

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += "\n FROM " + select_from + "\n"
    else:
        dbt_code += "\n FROM " + "{{" + select_from + "}}" + "\n"

    return dbt_code


def coalesce_columns(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    sql = coalesce_columns_dbt_sql(config, warehouse)

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, sql)

    return model_sql_path
