from logging import basicConfig, getLogger, INFO

from lib.dbtproject import dbtProject

basicConfig(level=INFO)
logger = getLogger()


def drop_columns(config: dict, warehouse: str, project_dir: str):
    dest_schema = config["dest_schema"]
    input_name = config["input_name"]
    columns = config.get("columns", [])

    model_code = f'{{{{ config(materialized="table") }}}}\n'
    columns = ','.join(columns)
    model_code += f'SELECT {{{{ dbt_utils.star(from=ref({input_name}), except=[{columns}]) }}}} FROM {{ref("{input_name}")}};\n'

    model_name = f"drop_{input_name}_columns"

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    dbtproject.write_model(dest_schema, model_name, model_code, subdir="staging")


def rename_columns(config: dict, warehouse: str, project_dir: str):
    input_name = config["input_name"]
    dest_schema = config["dest_schema"]
    columns = config.get("columns", {})
    output_name = config.get("output_name", input_name)

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    model_code += '{{ config(materialized="table") }}\n\n'
    model_code += f'SELECT \n'
    model_code += f'  {{ dbt_utils.star(from=ref("{input_name}")) }}, \n'
    
    for old_name, new_name in columns.items():
        model_code += f'  {old_name} AS "{new_name}", \n'

    model_code = model_code[:-3]  # Remove trailing comma and space
    model_code += f'\nFROM \n  {{ ref("{input_name}") }}'

    model_name = f"rename_{output_name}_columns"
    dbtproject.write_model(dest_schema, model_name, model_code, subdir="staging")