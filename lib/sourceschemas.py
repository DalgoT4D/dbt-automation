"""helpers for working with dbt source configs"""


# ================================================================================
def mksourcedefinition(sourcename: str, input_schema: str, tables: list):
    """generates the data structure for a dbt sources.yml"""
    airbyte_prefix = "_airbyte_raw_"

    source = {"name": sourcename, "schema": input_schema, "tables": []}

    for tablename in tables:
        cleaned_name = tablename.replace(airbyte_prefix, "")
        source["tables"].append(
            {
                "name": cleaned_name,
                "identifier": tablename,
                "description": "",
            }
        )

    sourcedefinitions = {
        "version": 2,
        "sources": [source],
    }
    return sourcedefinitions
