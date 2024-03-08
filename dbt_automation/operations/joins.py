"""
This operation implements the standard joins operation for dbt automation
"""

from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref


def joins_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """Given a regex and a column name, extract the regex from the column."""
    input_tables = config.get("input_arr", [])
    join_type: str = config.get("join_type", "")
    join_on = config.get("join_on", {})

    if join_type not in ["inner", "left"]:
        raise ValueError(f"join type not supported")

    if len(input_tables) != 2:
        raise ValueError(f"join operation requires exactly 2 input tables")

    aliases = ["t1", "t2"]

    # select
    dbt_code = f"\nSELECT "

    output_set = set()  # to check for duplicate column names
    for i, (alias, input_table) in enumerate(zip(aliases, input_tables)):
        source_columns = input_table["source_columns"]

        for col_name in source_columns:
            dbt_code += f"{quote_columnname(alias, warehouse.name)}.{quote_columnname(col_name, warehouse.name)}"
            if col_name in output_set:
                dbt_code += (
                    f" AS {quote_columnname(col_name + f'_{i+1}', warehouse.name)},\n"
                )
                output_set.add(col_name + f"_{i+1}")
            else:
                dbt_code += ",\n"
                output_set.add(col_name)

    dbt_code = dbt_code[:-2]

    select_from = source_or_ref(**input_tables[0]["input"])
    if input_tables[0]["input"]["input_type"] == "cte":
        dbt_code += "\n FROM " + select_from + " " + aliases[0] + "\n"
    else:
        dbt_code += "\n FROM " + "{{" + select_from + "}}" + " " + aliases[0] + "\n"

    # join
    dbt_code += (
        f" {join_type.upper()} JOIN "
        + "{{"
        + source_or_ref(**input_tables[1]["input"])
        + "}}"
        + f" {aliases[1]}"
        + "\n"
    )

    dbt_code += f" ON {quote_columnname(aliases[0], warehouse.name)}.{quote_columnname(join_on['key1'], warehouse.name)}"
    dbt_code += f" {join_on['compare_with']} {quote_columnname(aliases[1], warehouse.name)}.{quote_columnname(join_on['key2'], warehouse.name)}\n"

    return dbt_code, len(output_set)


def join(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Generate DBT model file for regex extraction.
    """
    dest_schema = config["dest_schema"]
    output_model_name = config["output_name"]
    dbt_sql = ""
    if config["input_arr"][0]["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = joins_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    model_sql_path = dbtproject.write_model(dest_schema, output_model_name, dbt_sql)

    return model_sql_path, output_cols
