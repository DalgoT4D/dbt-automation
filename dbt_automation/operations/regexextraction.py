"""extract from a regex"""

from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref


def regex_extraction(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """given a regex and a column name, extract the regex from the column"""
    dest_schema = config["dest_schema"]
    columns = config.get("columns", {})
    output_model_name = config["output_name"]

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    model_code = (
        f"{{{{ config(materialized='table', schema='{dest_schema}') }}}}\n\nSELECT "
    )

    for col_name, regex in columns.items():
        if warehouse.name == "postgres":
            model_code += f"""substring({quote_columnname(col_name, warehouse.name)}
                            FROM '{regex}') AS {quote_columnname(col_name, warehouse.name)}, """
        elif warehouse.name == "bigquery":
            model_code += f"""REGEXP_EXTRACT({quote_columnname(col_name, warehouse.name)}, r'{regex}')
                            AS {quote_columnname(col_name, warehouse.name)}, """

    model_code += (
        "{{ dbt_utils.star(from="
        + source_or_ref(**config["input"])
        + ", except=["
        + ", ".join([f'"{col}"' for col in columns.keys()])
        + "]) }}"
    )

    model_code += "\n FROM " + "{{" + source_or_ref(**config["input"]) + "}}" + "\n"

    model_sql_path = dbtproject.write_model(dest_schema, output_model_name, model_code)
    return model_sql_path
