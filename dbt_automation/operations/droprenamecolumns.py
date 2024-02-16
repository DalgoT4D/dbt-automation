"""drop and rename columns"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def drop_columns_sql(config: dict, warehouse: WarehouseInterface) -> str:
    """
    Generate SQL code for the drop_columns operation.
    """
    dest_schema = config["dest_schema"]
    columns = config.get("columns", [])
    output_model_name = config["output_name"]

    model_code = f"WITH drop_column AS (\n"
    model_code += (
        "SELECT {{dbt_utils.star(from="
        + source_or_ref(**config["input"])
        + ", except=["
    )
    model_code += ",".join([f'"{col}"' for col in columns])
    model_code += "])}}"
    model_code += " FROM " + "{{" + source_or_ref(**config["input"]) + "}}" + "\n"

    return model_code


def drop_columns(config: dict, warehouse: WarehouseInterface, project_dir: str) -> str:
    """
    Perform dropping of columns and generate a DBT model.
    """
    sql = drop_columns_sql(config, warehouse)

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

    dbt_code = ""

    if config["input"]["input_type"] != "cte":
        dbt_code += f"{{{{ config(materialized='table',schema='{dest_schema}') }}}}\n"

    dbt_code += (
        "SELECT {{dbt_utils.star(from="
        + source_or_ref(**config["input"])
        + ", except=["
    )
    dbt_code += ",".join([f'"{col}"' for col in columns.keys()])
    dbt_code += "])}} , "

    for old_name, new_name in columns.items():
        dbt_code += f"""{quote_columnname(old_name, warehouse.name)} AS {quote_columnname(new_name, warehouse.name)}, """

    dbt_code = dbt_code[:-2]
    dbt_code += " FROM " + "{{" + source_or_ref(**config["input"]) + "}}" + "\n"

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
