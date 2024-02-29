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
) -> str:
    """
    Generate SQL code by merging SQL code from multiple operations.
    """
    if not operations:
        return "-- No operations specified, no SQL generated."

    cte_sql_list = []
    output_cols = []  # return the last operations output columns

    # push select statements into the queue
    for cte_counter, operation in enumerate(operations):

        if operation["type"] == "castdatatypes":
            op_select_statement, out_cols = cast_datatypes_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "arithmetic":
            op_select_statement, out_cols = arithmetic_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "coalescecolumns":
            op_select_statement, out_cols = coalesce_columns_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "concat":
            op_select_statement, out_cols = concat_columns_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "dropcolumns":
            op_select_statement, out_cols = drop_columns_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "renamecolumns":
            op_select_statement, out_cols = rename_columns_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "flattenjson":
            op_select_statement, out_cols = flattenjson_dbt_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "regexextraction":
            op_select_statement, out_cols = regex_extraction_sql(
                operation["config"], warehouse
            )
        elif operation["type"] == "union_tables":
            op_select_statement, out_cols = union_tables_sql(
                operation["config"], warehouse
            )

        output_cols = out_cols

        cte_sql = f" , {operation['as_cte']} as (\n"
        if cte_counter == 0:
            cte_sql = f"WITH {operation['as_cte']} as (\n"
        cte_sql += op_select_statement
        cte_sql += f")"

        # last step
        if cte_counter == len(operations) - 1:
            prev_as_cte = operations[cte_counter]["as_cte"]
            cte_sql += "\n-- Final SELECT statement combining the outputs of all CTEs\n"
            cte_sql += f"SELECT *\nFROM {prev_as_cte}"

        cte_sql_list.append(cte_sql)

    return "".join(cte_sql_list), output_cols


def merge_operations(
    config: dict,
    warehouse: WarehouseInterface,
    project_dir: str,
):
    """
    Perform merging of operations and generate a DBT model.
    """

    dbt_sql = (
        "{{ config(materialized='table', schema='" + config["dest_schema"] + "') }}\n"
    )

    for i, operation in enumerate(config["operations"]):
        operation["as_cte"] = f"cte{i+1}"  # this will go as WITH cte1 as (...)
        if i == 0:
            # first operation input can be model or source
            operation["config"]["input"] = config["input"]
        else:
            # select the previous as_cte as source for next operations
            operation["config"]["input"] = {
                "input_name": config["operations"][i - 1]["as_cte"],
                "input_type": "cte",
                "source_name": None,
            }

    select_statement, output_cols = merge_operations_sql(
        config["operations"],
        warehouse,
    )
    dbt_sql += select_statement

    dbt_project = dbtProject(project_dir)
    dbt_project.ensure_models_dir(config["dest_schema"])

    model_sql_path = dbt_project.write_model(
        config["dest_schema"], config["output_name"], dbt_sql
    )

    return model_sql_path, output_cols
