"""helpers to create .yml dbt files"""


# ================================================================================
def mk_model_config(schemaname: str, modelname_: str, columnspec: list):
    """creates a model config with the given column spec"""
    columns = [
        {
            "name": "_airbyte_ab_id",
            "description": "",
            "tests": ["unique", "not_null"],
        }
    ]
    for column in columnspec:
        columns.append(
            {
                "name": column,
                "description": "",
            }
        )
    return {
        "name": modelname_,
        "description": "",
        "+schema": schemaname,
        "columns": columns,
    }
