"""generates a model which coalesces columns"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface


basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def coalesce_columns(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """coalesces columns"""
    dest_schema = config["dest_schema"]
    output_name = config["output_name"]
    input_name = config["input_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    union_code = f"{{{{ config(materialized='table', schema='{dest_schema}') }}}}\n"

    columns = config["columns"]
    columnnames = [c["columnname"] for c in columns]
    union_code += "SELECT {{dbt_utils.star(from=ref('" + input_name + "'), except=["
    union_code += ",".join([f'"{columnname}"' for columnname in columnnames])
    union_code += "])}}"

    union_code += ", COALESCE("

    for column in config["columns"]:
        union_code += quote_columnname(column["columnname"], warehouse.name) + ", "
    union_code = union_code[:-2] + ") AS " + config["output_column_name"]

    union_code += " FROM " + "{{ref('" + input_name + "')}}" + "\n"

    logger.info(f"writing dbt model {union_code}")
    model_sql_path = dbtproject.write_model(
        dest_schema,
        output_name,
        union_code,
    )
    logger.info(f"dbt model {output_name} successfully created")

    return model_sql_path
