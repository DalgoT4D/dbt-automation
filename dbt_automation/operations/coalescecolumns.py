"""generates a model which coalesces columns"""

from logging import basicConfig, getLogger, INFO

from lib.dbtproject import dbtProject
from lib.columnutils import quote_columnname

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def coalesce_columns(config: dict, warehouse: str, project_dir: str):
    """coalesces columns"""
    dest_schema = config["dest_schema"]
    output_name = config["output_name"]
    input_name = config["input_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    union_code = "{{ config(materialized='table',) }}\n"

    columns = config["columns"]
    columnnames = [c["columnname"] for c in columns]
    union_code += "SELECT {{dbt_utils.star(from=ref('" + input_name + "'), except=["
    union_code += ",".join([f'"{columnname}"' for columnname in columnnames])
    union_code += "])}}"

    union_code += ", COALESCE("

    for column in config["columns"]:
        union_code += quote_columnname(column["columnname"], warehouse) + ", "
    union_code = union_code[:-2] + ") AS " + config["output_column_name"]

    union_code += " FROM " + "{{ref('" + input_name + "')}}" + "\n"

    logger.info(f"writing dbt model {union_code}")
    dbtproject.write_model(
        dest_schema,
        output_name,
        union_code,
    )
    logger.info(f"dbt model {output_name} successfully created")
