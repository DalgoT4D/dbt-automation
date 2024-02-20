from typing import List
from dbt_automation.operations.arithmetic import arithmetic_dbt_sql
from dbt_automation.operations.coalescecolumns import (
    coalesce_columns_dbt_sql,
)
from dbt_automation.operations.concatcolumns import concat_columns_dbt_sql
from dbt_automation.operations.droprenamecolumns import (
    drop_columns_dbt_sql,
    rename_columns_dbt_sql,
)
from dbt_automation.operations.flattenjson import flattenjson_dbt_sql
from dbt_automation.operations.mergetables import union_tables_sql
from dbt_automation.operations.regexextraction import regex_extraction_sql
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation.operations.castdatatypes import cast_datatypes_sql
from dbt_automation.utils.tableutils import source_or_ref


def merge_operations_sql(
    operations: List[dict],
    warehouse: WarehouseInterface,
    source_name: str,
) -> str:
    """
    Generate SQL code by merging SQL code from multiple operations.
    """
    if not operations:
        return "-- No operations specified, no SQL generated."

    cte_sql_list = []
    cte_counter = 1

    config_sql = "{{ config(materialized='table') }}"
    cte_sql_list.append(config_sql)

    config_sql_added = False  # Flag to track if config_sql is added

    for idx, operation in enumerate(operations):
        cte_name = f"cte{cte_counter}"
        cte_counter += 1

        cte_sql = f"{cte_name} as (\n"

        if operation["type"] == "cte":
            cte_sql += f"{operation['config']['sql']}\n"
        else:
            if not config_sql_added and idx == 0:
                config_sql_added = True

            if operation["type"] == "castdatatypes":
                cte_sql += cast_datatypes_sql(operation["config"], warehouse, "")
            elif operation["type"] == "arithmetic":
                cte_sql += arithmetic_dbt_sql(operation["config"], warehouse, "")
            elif operation["type"] == "coalescecolumns":
                cte_sql += coalesce_columns_dbt_sql(operation["config"], warehouse, "")
            elif operation["type"] == "concat":
                cte_sql += concat_columns_dbt_sql(operation["config"], warehouse, "")
            elif operation["type"] == "dropcolumns":
                cte_sql += drop_columns_dbt_sql(operation["config"], warehouse, "")
            elif operation["type"] == "renamecolumns":
                cte_sql += rename_columns_dbt_sql(operation["config"], warehouse, "")
            elif operation["type"] == "flattenjson":
                cte_sql += flattenjson_dbt_sql(operation["config"], warehouse, "")
            elif operation["type"] == "regexextraction":
                cte_sql += regex_extraction_sql(operation["config"], warehouse, "")
            elif operation["type"] == "union_tables":
                cte_sql += union_tables_sql(operation["config"], warehouse, "")

        cte_sql += ")"
        cte_sql_list.append(cte_sql)

    cte_sql_list[1] = cte_sql_list[1].replace("cte1", f"WITH cte1")

    if not cte_sql_list:
        return "-- No SQL code generated for any operation."

    for i in range(1, len(cte_sql_list)):
        if "input" not in operations[i - 1]["config"]:
            continue  # Skip this iteration if 'input' key is missing
        previous_cte_name = f"cte{i - 1}"
        select_from = source_or_ref(**operations[i - 1]["config"]["input"])
        cte_sql_list[i] = cte_sql_list[i].replace(
            f"{select_from}", f"{previous_cte_name}"
        )

    cte_sql_list[1] = cte_sql_list[1].replace("cte0", f"{{{{ref('{source_name}')}}}}")

    sql = ",\n".join(cte_sql_list[1:]) + "\n\n"
    sql = f"{cte_sql_list[0]}\n{sql}"

    last_output_name = f"cte{len(cte_sql_list) - 1}"
    sql += "-- Final SELECT statement combining the outputs of all CTEs\n"
    sql += f"SELECT *\nFROM {last_output_name}"

    return sql


def merge_operations(
    config: List[dict],
    warehouse: WarehouseInterface,
    project_dir: str,
) -> str:
    """
    Perform merging of operations and generate a DBT model.
    """

    sql = merge_operations_sql(
        config["operations"],
        warehouse,
        source_name=config["operations"][0]["config"]["input"]["source_name"],
    )

    destination_schema = config["operations"][0]["config"].get(
        "dest_schema", "intermediate"
    )
    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(destination_schema)

    model_sql_path = dbt_project.write_model("intermediate", "merged_operations", sql)

    return model_sql_path
