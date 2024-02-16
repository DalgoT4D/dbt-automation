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
def flattenjson_dbt_sql(config: dict, warehouse: WarehouseInterface) -> str:
    """
    source_schema: name of the input schema
    input: input dictionary check operations.yaml.template
    dest_schema: name of the output schema
    output_name: name of the output model
    columns_to_copy: list of columns to copy from the input model
    json_column: name of the json column to flatten
    json_columns_to_copy: list of columns to copy from the json_column
    """
    dest_schema = config["dest_schema"]
    output_name = config["output_name"]
    columns_to_copy = config["columns_to_copy"]
    json_column = config["json_column"]
    json_columns_to_copy = config["json_columns_to_copy"]

    model_code = f"WITH {output_name} AS (\n"
    if columns_to_copy == "*":
        model_code += "SELECT *\n"
    else:
        model_code += f"SELECT {', '.join([quote_columnname(col, warehouse.name) for col in columns_to_copy])}\n"

    # json_columns = warehouse.get_json_columnspec(source_schema, input_name, json_column)

    # convert to sql-friendly column names
    sql_columns = make_cleaned_column_names(json_columns_to_copy)

    # after cleaning we may have duplicates
    sql_columns = dedup_list(sql_columns)

    for json_field, sql_column in zip(json_columns_to_copy, sql_columns):
        model_code += (
            "," + warehouse.json_extract_op(json_column, json_field, sql_column) + "\n"
        )

    model_code += " FROM " + "{{" + source_or_ref(**config["input"]) + "}}" + "\n"

    return model_code


def flattenjson(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Flatten JSON columns.
    """
    sql_code = flattenjson_dbt_sql(config, warehouse)

    dest_schema = config["dest_schema"]
    output_name = config["output_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    model_sql_path = dbtproject.write_model(dest_schema, output_name, sql_code)

    return model_sql_path
