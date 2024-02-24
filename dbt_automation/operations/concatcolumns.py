"""
This file takes care of dbt string concat operations
"""

from logging import basicConfig, getLogger, INFO
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


def concat_columns_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
) -> str:
    """
    Generate SQL code for the concat_columns operation.
    """
    output_column_name = config["output_column_name"]
    columns = config["columns"]
    source_columns = config["source_columns"]

    columns_to_concat = [
        col["name"] for col in columns if col.get("is_col", "yes") == "yes"
    ]

    concat_fields = ",".join(
        [quote_columnname(col, warehouse.name) for col in columns_to_concat]
    )

    dbt_code = "SELECT " + ", ".join(
        [quote_columnname(col, warehouse.name) for col in source_columns]
    )
    dbt_code += f", CONCAT({concat_fields}) AS {quote_columnname(output_column_name, warehouse.name)}"

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += f"\nFROM {select_from}\n"
    else:
        dbt_code += f"\nFROM {{{{ {select_from} }}}}\n"

    return dbt_code


def concat_columns(
    config: dict, warehouse: WarehouseInterface, project_dir: str
) -> str:
    """
    Perform concatenation of columns and generate a DBT model.
    """
    config_sql = ""
    if config["input"]["input_type"] != "cte":
        config_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    sql = config_sql + "\n" + concat_columns_dbt_sql(config, warehouse)

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    model_sql_path = dbt_project.write_model(
        config["dest_schema"], config["output_name"], sql
    )

    return model_sql_path
