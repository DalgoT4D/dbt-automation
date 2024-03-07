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

    if join_type not in ["inner", "left"]:
        raise ValueError(f"join type not supported")

    if len(input_tables) != 2:
        raise ValueError(f"join operation requires exactly 2 input tables")

    alias = ["t1", "t2"]

    source_columns_table1 = set(input_tables[0]["source_columns"])
    source_columns_table2 = set(input_tables[1]["source_columns"])

    # Find intersection of source columns
    intersecting_columns = source_columns_table1 & source_columns_table2

    # Add "_2" to intersecting column names in second table
    input_tables[1]["source_columns"] = [
        col + "_2" if col in intersecting_columns else col
        for col in input_tables[1]["source_columns"]
    ]

    # select
    dbt_code = f"\nSELECT "

    for alias, input_table in zip(alias, input_tables):
        source_columns = input_table["source_columns"]

        for col_name in source_columns:
            dbt_code += f"{quote_columnname(alias, warehouse.name)}.{quote_columnname(col_name, warehouse.name)},\n"

    dbt_code = dbt_code[:-2] + f"\n"

    select_from = source_or_ref(**config["input"])
    if config["input"]["input_type"] == "cte":
        dbt_code += "\n FROM " + select_from + f" {alias[0]}" + "\n"
    else:
        dbt_code += "\n FROM " + "{{" + select_from + "}}" + f" {alias[0]}" + "\n"

    # join
    dbt_code += (
        f" {join_type.upper()} JOIN "
        + "{{"
        + source_or_ref(**input_tables[1])
        + "}}"
        + f" {alias[1]}"
        + "\n"
    )

    # TODO:

    return (
        dbt_code,
        input_tables[0]["source_columns"] + input_tables[1]["source_columns"],
    )


def join(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Generate DBT model file for regex extraction.
    """
    dest_schema = config["dest_schema"]
    output_model_name = config["output_name"]
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = joins_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbtproject = dbtProject(project_dir)
    dbtproject.ensure_models_dir(dest_schema)

    model_sql_path = dbtproject.write_model(dest_schema, output_model_name, dbt_sql)

    return model_sql_path, output_cols
