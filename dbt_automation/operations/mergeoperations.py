from typing import List
from dbt_automation.operations.arithmetic import arithmetic_dbt_sql
from dbt_automation.operations.coalescecolumns import (
    coalesce_columns,
    coalesce_columns_dbt_sql,
)
from dbt_automation.operations.concatcolumns import concat_columns_dbt_sql
from dbt_automation.operations.droprenamecolumns import (
    drop_columns_sql,
    rename_columns_dbt_sql,
)
from dbt_automation.operations.flattenjson import flattenjson_dbt_sql
from dbt_automation.operations.mergetables import union_tables_sql
from dbt_automation.operations.regexextraction import regex_extraction_sql
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.operations.castdatatypes import cast_datatypes_sql


def merge_operations_sql(operations: List[dict], warehouse: WarehouseInterface) -> str:
    """
    Generate SQL code by merging SQL code from multiple operations.
    """
    if not operations:
        return "-- No operations specified, no SQL generated."

    cte_sql_list = []
    cte_sql_list.append("{{ config(materialized='table',schema='intermediate') }}")

    for operation in operations:
        if operation["type"] == "castdatatypes":
            cast_sql = cast_datatypes_sql(operation["config"], warehouse)
            cte_sql_list.append(cast_sql)

        elif operation["type"] == "arithmetic":
            arithmetic_sql = arithmetic_dbt_sql(operation["config"])
            cte_sql_list.append(arithmetic_sql)

        elif operation["type"] == "coalescecolumns":
            coalesce_sql = coalesce_columns_dbt_sql(operation["config"], warehouse)
            cte_sql_list.append(coalesce_sql)

        elif operation["type"] == "concat":
            concat_sql = concat_columns_dbt_sql(operation["config"], warehouse)
            cte_sql_list.append(concat_sql)

        elif operation["type"] == "dropcolumns":
            drop_sql = drop_columns_sql(operation["config"], warehouse)
            cte_sql_list.append(drop_sql)

        elif operation["type"] == "renamecolumns":
            rename_sql = rename_columns_dbt_sql(operation["config"], warehouse)
            cte_sql_list.append(rename_sql)

        elif operation["type"] == "flattenjson":
            flatten_json_sql = flattenjson_dbt_sql(operation["config"], warehouse)
            cte_sql_list.append(flatten_json_sql)

        elif operation["type"] == "regexextraction":
            regex_sql = regex_extraction_sql(operation["config"], warehouse)
            cte_sql_list.append(regex_sql)

        elif operation["type"] == "union_tables":
            # Generate SQL for union_tables operation
            union_sql = union_tables_sql(operation["config"], warehouse)
            cte_sql_list.append(union_sql)

    if not cte_sql_list:
        return "-- No SQL code generated for any operation."

    sql = f",\n".join(cte_sql_list) + "\n\n"

    if cte_sql_list:
        last_output_name = operations[-1]["config"]["output_name"]
        sql += f"-- Final SELECT statement combining the outputs of all CTEs\n"
        sql += f"SELECT *\nFROM {last_output_name}"
    else:
        sql += "-- No operations specified, no SQL generated."

    return sql


def merge_operations(
    config: List[dict], warehouse: WarehouseInterface, project_dir: str
) -> str:
    """
    Perform merging of operations and generate a DBT model.
    """
    sql = merge_operations_sql(config["operations"], warehouse)

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir("intermediate")  # Example destination schema

    model_sql_path = dbt_project.write_model("intermediate", "merged_operations", sql)

    return model_sql_path
