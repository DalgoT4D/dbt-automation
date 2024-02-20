"""generates a model which coalesces columns"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def coalesce_columns_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
    config_sql: str,
) -> str:
    """
    Generate SQL code for the coalesce_columns operation.
    """
    dest_schema = config["dest_schema"]
    columns = config.get("columns", [])
    source_columns = config["source_columns"]
    output_name = config["output_name"]

    dbt_code = ""

    dbt_code = config_sql + "\n"

    dbt_code += "SELECT\n"

    select_from = source_or_ref(**config["input"])

    for column in source_columns:
        if column in [c["columnname"] for c in columns]:
            dbt_code += f"COALESCE({quote_columnname(column, warehouse.name)}, NULL) AS {quote_columnname(column, warehouse.name)},\n"
        else:
            dbt_code += f"{quote_columnname(column, warehouse.name)},\n"

    dbt_code += (
        f"COALESCE("
        + ", ".join(
            [quote_columnname(c["columnname"], warehouse.name) for c in columns]
        )
        + f") AS {quote_columnname(output_name, warehouse.name)}\n"
    )

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += f"FROM {select_from}\n"
    else:
        dbt_code += f"FROM {{{{{select_from}}}}}\n"

    return dbt_code


def coalesce_columns(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    config_sql = ""
    if config["input"]["input_type"] != "cte":
        config_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    sql = coalesce_columns_dbt_sql(config, warehouse, config_sql)

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, sql)

    return model_sql_path
