"""generates a model which casts columns to specified SQL data types"""

from logging import basicConfig, getLogger, INFO

from dbt_automation.lib.dbtproject import dbtProject
from dbt_automation.lib.columnutils import quote_columnname

basicConfig(level=INFO)
logger = getLogger()

WAREHOUSE_COLUMN_TYPES = {
    "postgres": {},
    "bigquery": {},
}


# pylint:disable=logging-fstring-interpolation
def cast_datatypes(config: dict, warehouse: str, project_dir: str):
    """generates the model"""
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

    for column in columns:
        warehouse_column_type = WAREHOUSE_COLUMN_TYPES[warehouse].get(
            column["columntype"], column["columntype"]
        )
        union_code += (
            ", CAST("
            + quote_columnname(column["columnname"], warehouse)
            + " AS "
            + warehouse_column_type
            + ") AS "
            + quote_columnname(column["columnname"], warehouse)
        )

    union_code += " FROM " + "{{ref('" + input_name + "')}}" + "\n"

    logger.info(f"writing dbt model {union_code}")
    dbtproject.write_model(
        dest_schema,
        output_name,
        union_code,
    )
    logger.info(f"dbt model {output_name} successfully created")
