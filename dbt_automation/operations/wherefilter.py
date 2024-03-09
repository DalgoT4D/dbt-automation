"""generates a model which filters using the sql where clause"""

import datetime
from logging import basicConfig, getLogger, INFO

from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.columnutils import quote_columnname, quote_constvalue
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.utils.tableutils import source_or_ref

basicConfig(level=INFO)
logger = getLogger()


# pylint:disable=unused-argument,logging-fstring-interpolation
def where_filter_sql(
    config: dict,
    warehouse: WarehouseInterface,
):
    """
    Generate SQL code for the coalesce_columns operation.
    """
    source_columns = config["source_columns"]
    input_table = config["input"]
    clauses: dict = config.get("clauses", {})
    and_clauses: list[dict] = clauses.get("and", [])
    or_clauses: list[dict] = clauses.get("or", [])
    sql_snippet: list[dict] = clauses.get("sql_snippet", "")

    dbt_code = "SELECT\n"

    select_from = source_or_ref(**input_table)

    for col_name in source_columns:
        dbt_code += f"{quote_columnname(col_name, warehouse.name)},\n"

    select_from = source_or_ref(**input_table)
    if input_table["input_type"] == "cte":
        dbt_code += f"FROM {select_from}\n"
    else:
        dbt_code += f"FROM {{{{{select_from}}}}}\n"

    # where
    if not (len(and_clauses) > 0 or len(or_clauses) > 0 or len(sql_snippet) > 0):
        raise ValueError("No where clause provided")

    dbt_code += "WHERE ("
    if len(and_clauses) > 0:
        temp = []
        for clause in and_clauses:
            clause = (
                f"{quote_columnname(clause['column'], warehouse.name)} "
                + f"{clause['operator']} "
                + f"{quote_constvalue(str(clause['value']), warehouse.name)} "
            )
            temp.append(clause)

        dbt_code += " AND ".join(temp)

    elif len(or_clauses) > 0:
        temp = []
        for clause in or_clauses:
            clause = (
                f"{quote_columnname(clause['column'], warehouse.name)} "
                + f"{clause['operator']} "
                + f"{quote_constvalue(str(clause['value']), warehouse.name)} "
            )
            temp.append(clause)

        dbt_code += " OR ".join(temp)

    elif sql_snippet:
        dbt_code += f"{sql_snippet}"

    dbt_code += ")"

    return dbt_code, source_columns


def where_filter(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """
    Perform coalescing of columns and generate a DBT model.
    """
    dbt_sql = ""
    if config["input"]["input_type"] != "cte":
        dbt_sql = (
            "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}"
        )

    select_statement, output_cols = where_filter_sql(config, warehouse)
    dbt_sql += "\n" + select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    output_name = config["output_name"]
    dest_schema = config["dest_schema"]
    model_sql_path = dbt_project.write_model(dest_schema, output_name, dbt_sql)

    return model_sql_path, output_cols
