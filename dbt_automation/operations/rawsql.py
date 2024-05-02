from sql_metadata import Parser

from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

def raw_generic_dbt_sql(
    config: str,
    warehouse: WarehouseInterface,
):
    """
    Parses the given SQL statement to extract tables and columns and generates DBT code.
    """
    source_columns = config.get('source_columns', [])
    sql_statement = config.get('raw_sql')
    parser = Parser(sql_statement)
    tables = parser.tables
    columns = parser.columns

    if columns == "*":
        dbt_code = "SELECT *"
    else:
        dbt_code = f"SELECT {', '.join([quote_columnname(col, warehouse.name) for col in columns])}"

    if len(tables) == 1:
        config['input']['input_name'] = tables[0]
        select_from = source_or_ref(tables[0], tables[0], "source")
    else:
        select_from = " JOIN ".join([f"{{{{ source('{table}', '{table}') }}}}" for table in tables])

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += "\n FROM " + select_from + "\n"
    else:
        dbt_code += "\n FROM " + "{{" + select_from + "}}" + "\n"

    return dbt_code, source_columns

def generic_sql_function(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform a generic SQL function operation.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = raw_generic_dbt_sql(config, warehouse)

    dest_schema = config["dest_schema"]
    output_name = config["output_model_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)
    model_sql_path = dbtproject.write_model(dest_schema, output_name, dbt_sql + select_statement)

    return model_sql_path, output_cols
