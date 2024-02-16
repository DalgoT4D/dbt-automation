"""generates a model which casts columns to specified SQL data types"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref


basicConfig(level=INFO)
logger = getLogger()

WAREHOUSE_COLUMN_TYPES = {
    "postgres": {},
    "bigquery": {},
}


# pylint:disable=logging-fstring-interpolation
def cast_datatypes_sql(config: dict, warehouse: WarehouseInterface) -> str:
    """
    Generate SQL code for the cast_datatypes operation.
    """
    dest_schema = config["dest_schema"]
    columns = config.get("columns", [])
    output_name = config["output_name"]

    columns = config["columns"]
    columnnames = [c["columnname"] for c in columns]
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

    for column in columns:
        warehouse_column_type = WAREHOUSE_COLUMN_TYPES[warehouse.name].get(
            column["columntype"], column["columntype"]
        )
        dbt_code += (
            ", CAST("
            + quote_columnname(column["columnname"], warehouse.name)
            + " AS "
            + warehouse_column_type
            + ") AS "
            + quote_columnname(column["columnname"], warehouse.name)
        )

    dbt_code += " FROM " + "{{" + source_or_ref(**config["input"]) + "}}" + "\n"

    return dbt_code


def cast_datatypes(
    config: dict, warehouse: WarehouseInterface, project_dir: str
) -> str:
    """
    Perform casting of data types and generate a DBT model.
    """
    sql = cast_datatypes_sql(config, warehouse)
    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, sql)

    return model_sql_path
