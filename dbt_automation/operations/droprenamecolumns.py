"""drop and rename columns"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def drop_columns_dbt_sql(config: dict, warehouse: WarehouseInterface) -> str:
    """
    Generate SQL code for dropping columns from the source.
    """
    dest_schema = config["dest_schema"]
    columns = config.get("columns", [])
    source_columns = config["source_columns"]

    columns_to_drop = [col for col in source_columns if col not in columns]

    dbt_code = ""

    if config["input"]["input_type"] != "cte":
        dbt_code += f"{{{{ config(materialized='table',schema='{dest_schema}') }}}}\n"
    dbt_code += "SELECT " + ", ".join(
        [quote_columnname(col, warehouse.name) for col in columns_to_drop]
    )

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += f"\nFROM {select_from}\n"
    else:
        dbt_code += f"\nFROM {{{{ {select_from} }}}}\n"

    return dbt_code


def drop_columns(config: dict, warehouse: WarehouseInterface, project_dir: str) -> str:
    """
    Perform dropping of columns and generate a DBT model.
    """
    sql = drop_columns_dbt_sql(config, warehouse)

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_model_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_model_name, sql)

    return model_sql_path


def rename_columns_dbt_sql(config: dict, warehouse: WarehouseInterface) -> str:
    """Generate SQL code for renaming columns in a model."""
    dest_schema = config["dest_schema"]
    columns = config.get("columns", {})
    source_columns = config["source_columns"]

    dbt_code = ""

    if config["input"]["input_type"] != "cte":
        dbt_code += f"{{{{ config(materialized='table',schema='{dest_schema}') }}}}\n"

    dbt_code += "SELECT " + ", ".join([f'"{col}"' for col in source_columns]) + ", "

    for old_name, new_name in columns.items():
        dbt_code += f"{quote_columnname(old_name, warehouse.name)} AS {quote_columnname(new_name, warehouse.name)}, "

    dbt_code = dbt_code[:-2]  # Remove trailing comma and space
    select_from = source_or_ref(**config["input"])

    if config["input"]["input_type"] == "cte":
        dbt_code += "\n FROM " + select_from + "\n"
    else:
        dbt_code += "\n FROM " + "{{" + select_from + "}}" + "\n"

    return dbt_code


def rename_columns(
    config: dict, warehouse: WarehouseInterface, project_dir: str
) -> str:
    """Perform renaming of columns and generate a DBT model."""
    dest_schema = config["dest_schema"]
    output_model_name = config["output_name"]

    sql = rename_columns_dbt_sql(config, warehouse)

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    model_sql_path = dbtproject.write_model(dest_schema, output_model_name, sql)

    return model_sql_path
