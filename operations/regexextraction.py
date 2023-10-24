from lib.columnutils import quote_columnname
from lib.dbtproject import dbtProject

def regex_extraction(config: dict, warehouse: str, project_dir: str):
    input_name = config["input_name"]
    dest_schema = config["dest_schema"]
    columns = config.get("columns", {})
    output_model_name = config["output_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    model_code = '{{ config(materialized="table") }}\n\nSELECT '
    
    for col_name, regex in columns.items():
        model_code += f"substring({quote_columnname(col_name, warehouse)} from '{regex}') AS {quote_columnname(col_name, warehouse)}, "

    model_code += '{{ dbt_utils.star(from=ref("' + input_name + '"), except=[' + ', '.join([f'"{col}"' for col in columns.keys()]) + ']) }}'
    
    model_code += f'\nFROM \n  {{{{ ref("{input_name}") }}}}'
    
    dbtproject.write_model(dest_schema, output_model_name, model_code)
