"""
This file takes care of dbt string concat operations
"""

from logging import basicConfig, getLogger, INFO
from lib.dbtproject import dbtProject
from lib.columnutils import quote_columnname


basicConfig(level=INFO)
logger = getLogger()


def concat_columns(config: dict, warehouse: str, project_dir: str):
    """This function generates dbt model to concat strings"""

    dest_schema = config["dest_schema"]
    output_name = config["output_name"]
    input_name = config["input_name"]
    output_column_name = config["output_column_name"]
    columns = config["columns"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    dbt_code = "{{ config(materialized='table', schema='" + dest_schema + "') }}\n"
    concat_fields = ",".join(
        [
            quote_columnname(col["name"], warehouse)
            if col["is_col"] in ["yes", True, "y"]
            else f"'{col['name']}'"
            for col in columns
        ]
    )
    dbt_code += f"SELECT *, CONCAT({concat_fields}) AS {output_column_name}"
    dbt_code += " FROM " + "{{ref('" + input_name + "')}}" + "\n"

    dbtproject.write_model(dest_schema, output_name, dbt_code)
