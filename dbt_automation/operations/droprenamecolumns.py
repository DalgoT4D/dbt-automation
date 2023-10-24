from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname

basicConfig(level=INFO)
logger = getLogger()


def drop_columns(config: dict, warehouse: str, project_dir: str):
    dest_schema = config["dest_schema"]
    input_name = config["input_name"]
    columns = config.get("columns", [])
    output_model_name = config["output_name"]

    model_code = f'{{{{ config(materialized="table", schema="{dest_schema}") }}}}\n'
    columns = ",".join([f'"{col}"' for col in columns])
    model_code += f'SELECT {{{{ dbt_utils.star(from=ref("{input_name}"), except=[{columns}]) }}}} FROM {{{{ref("{input_name}")}}}}\n'

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    dbtproject.write_model(dest_schema, output_model_name, model_code)


def rename_columns(config: dict, warehouse: str, project_dir: str):
    input_name = config["input_name"]
    dest_schema = config["dest_schema"]
    columns = config.get("columns", {})
    output_model_name = config["output_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    model_code = '{{ config(materialized="table") }}\n\n'
    exclude_cols = ",".join([f'"{col}"' for col in columns.keys()])
    model_code += f'SELECT {{{{ dbt_utils.star(from=ref("{input_name}"),except=[{exclude_cols}]) }}}}, '

    for old_name, new_name in columns.items():
        model_code += f"{quote_columnname(old_name, warehouse)} AS {quote_columnname(new_name, warehouse)}, "

    model_code = model_code[:-2]  # Remove trailing comma and space
    model_code += f'\nFROM \n  {{{{ ref("{input_name}") }}}}'

    dbtproject.write_model(dest_schema, output_model_name, model_code)
