"""explode elements out of a json list into their own rows"""
from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.columnutils import make_cleaned_column_names, dedup_list

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def explodejsonarray(config: dict, warehouse, project_dir: str):
    """
    source_schema: name of the input schema
    input_name: name of the input model
    dest_schema: name of the output schema
    output_model: name of the output model
    columns_to_copy: list of columns to copy from the input model
    json_column: name of the json column to flatten
    """

    source_schema = config["source_schema"]
    source_table = config["source_table"]
    input_model = config.get("input_model")
    input_source = config.get("input_source")
    if input_model is None and input_source is None:
        raise ValueError("either input_model or input_source must be specified")
    dest_schema = config["dest_schema"]
    output_model = config["output_model"]
    columns_to_copy = config["columns_to_copy"]
    json_column = config["json_column"]

    model_code = f'{{{{ config(materialized="table", schema="{dest_schema}") }}}}'
    model_code += "\n"

    if columns_to_copy is None:
        model_code += "SELECT "
    elif columns_to_copy == "*":
        model_code += "SELECT *, "
    else:
        select_list = [quote_columnname(col, warehouse.name) for col in columns_to_copy]
        model_code += f"SELECT {', '.join(select_list)}, "

    model_code += "\n"

    json_columns = warehouse.get_json_columnspec_from_array(
        source_schema, source_table, json_column
    )

    # convert to sql-friendly column names
    sql_columns = make_cleaned_column_names(json_columns)

    # after cleaning we may have duplicates
    sql_columns = dedup_list(sql_columns)

    if warehouse.name == "postgres":
        select_list = []
        for json_field, sql_column in zip(json_columns, sql_columns):
            select_list.append(
                warehouse.json_extract_from_array_op(
                    json_column, json_field, sql_column
                )
            )
        model_code += ",".join(select_list)
        model_code += "\nFROM\n"

        if input_model:
            model_code += "{{ref('" + input_model + "')}}"
        else:
            model_code += "{{source('" + source_schema + "', '" + input_source + "')}}"

        model_code += "\n"

    elif warehouse.name == "bigquery":
        select_list = []
        for json_field, sql_column in zip(json_columns, sql_columns):
            select_list.append(
                warehouse.json_extract_op("JVAL", json_field, sql_column)
            )
        model_code += ",".join(select_list)
        model_code += "\nFROM\n"

        if input_model:
            model_code += "{{ref('" + input_model + "')}}"
        else:
            model_code += "{{source('" + source_schema + "', '" + input_source + "')}}"

        model_code += f""" CROSS JOIN UNNEST((
            SELECT JSON_EXTRACT_ARRAY(`{json_column}`, '$')
            FROM """

        if input_model:
            model_code += "{{ref('" + input_model + "')}}"
        else:
            model_code += "{{source('" + source_schema + "', '" + input_source + "')}}"

        model_code += ")) `JVAL`"

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    dbtproject.write_model(dest_schema, output_model, model_code)
