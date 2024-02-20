"""pull fields out of a json field into their own columns"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.columnutils import make_cleaned_column_names, dedup_list
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def flattenjson_dbt_sql(
    config: dict,
    warehouse: WarehouseInterface,
    config_sql: str,
) -> str:
    """
    source_schema: name of the input schema
    input: input dictionary check operations.yaml.template
    dest_schema: name of the output schema
    output_name: name of the output model
    source_columns: list of columns to copy from the input model
    json_column: name of the json column to flatten
    json_columns_to_copy: list of columns to copy from the json_column
    """
    dest_schema = config["dest_schema"]
    output_name = config["output_name"]
    source_columns = config["source_columns"]
    json_column = config["json_column"]
    json_columns_to_copy = config["json_columns_to_copy"]

    dbt_code = ""
    dbt_code = config_sql + "\n"

    if source_columns == "*":
        dbt_code += "SELECT *\n"
    else:
        dbt_code += f"SELECT {', '.join([quote_columnname(col, warehouse.name) for col in source_columns])}\n"

    # json_columns = warehouse.get_json_columnspec(source_schema, input_name, json_column)

    # convert to sql-friendly column names
    sql_columns = make_cleaned_column_names(json_columns_to_copy)

    # after cleaning we may have duplicates
    sql_columns = dedup_list(sql_columns)

    for json_field, sql_column in zip(json_columns_to_copy, sql_columns):
        dbt_code += (
            "," + warehouse.json_extract_op(json_column, json_field, sql_column) + "\n"
        )

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += "\n FROM " + select_from + "\n"
    else:
        dbt_code += "\n FROM " + "{{" + select_from + "}}" + "\n"

    return dbt_code


def flattenjson(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Flatten JSON columns.
    """
    config_sql = (
        "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
    )

    if config["input"]["input_type"] != "cte":
        config_sql = ""

    sql_code = flattenjson_dbt_sql(config, warehouse, config_sql)

    dest_schema = config["dest_schema"]
    output_name = config["output_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    model_sql_path = dbtproject.write_model(dest_schema, output_name, sql_code)

    return model_sql_path
