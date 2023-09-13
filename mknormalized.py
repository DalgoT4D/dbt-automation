"""creates the dbt models which call our flatten_json macro"""
import os
import argparse
from logging import basicConfig, getLogger, INFO
from string import Template
import yaml


basicConfig(level=INFO)
logger = getLogger()

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--project-dir", required=True)
parser.add_argument("--input-schema", default="staging", help="e.g. staging")
parser.add_argument("--output-schema", required=True, help="e.g. intermediate")
parser.add_argument("--output-schema-schemafile", required=True, help="e.g. schema.yml")
parser.add_argument(
    "--sources-file",
    required=True,
    help="filename must be relative to <project-dir>/models/<input-schema>/",
)
args = parser.parse_args()

# ================================================================================
# create the output directory
table_normalized_dbtmodel_dir = (
    f"{args.project_dir}/models/{args.output_schema}/normalized/"
)
if not os.path.exists(table_normalized_dbtmodel_dir):
    os.makedirs(table_normalized_dbtmodel_dir)
    logger.info("created directory %s", table_normalized_dbtmodel_dir)


# ================================================================================
NORMALIZED_MODEL_TEMPLATE = Template(
    """
{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]
) }}

{{
    flatten_json(
        model_name = source('$source_name', '$table_name'),
        json_column = '_airbyte_data'
    )
}}
"""
)


def mk_normalized_dbtmode(source_name: str, table_name: str) -> str:
    """creates a .sql dbt model"""
    return NORMALIZED_MODEL_TEMPLATE.substitute(
        {"source_name": source_name, "table_name": table_name}
    )


# ================================================================================
def process_source(src: dict, output_schema: str):
    """iterates through the tables in a source and creates their _normalized models"""
    for table in src["tables"]:
        logger.info(
            "[process_source] source_name=%s table_name=%s", src["name"], table["name"]
        )
        table_normalized_dbtmodel = mk_normalized_dbtmode(src["name"], table["name"])

        table_normalized_dbtmodel_filename = (
            table_normalized_dbtmodel_dir + src["name"] + "_normalized.sql"
        )

        logger.info("writing %s", table_normalized_dbtmodel_filename)
        with open(table_normalized_dbtmodel_filename, "w", encoding="utf-8") as outfile:
            outfile.write(table_normalized_dbtmodel)
            outfile.close()

    return {
        "name": src["name"],
        "description": "",
        "+schema": output_schema,
        "columns": [
            {
                "name": "_airbyte_ab_id",
                "description": "",
                "tests": ["unique", "not_null"],
            }
        ],
    }


# ================================================================================
sources_filename = f"{args.project_dir}/models/{args.input_schema}/{args.sources_file}"

output_schema_schema = {
    "version": 2,
    "models": [],
}

with open(sources_filename, "r", encoding="utf-8") as sources_file:
    sources = yaml.safe_load(sources_file)

    for source in sources["sources"]:
        if source["schema"] == "staging":
            output_schema_schema["models"].append(
                process_source(source, args.output_schema)
            )

output_schema_schemafilename = (
    f"{args.project_dir}/models/{args.output_schema}/{args.output_schema_schemafile}"
)
logger.info("writing %s", output_schema_schemafilename)
with open(
    output_schema_schemafilename, "w", encoding="utf-8"
) as output_schema_schemafile:
    yaml.safe_dump(output_schema_schema, output_schema_schemafile, sort_keys=False)
