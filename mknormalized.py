"""creates the dbt models which call our flatten_json macro"""
import os
import argparse
from logging import basicConfig, getLogger, INFO
from string import Template
from pathlib import Path
import yaml


basicConfig(level=INFO)
logger = getLogger()

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--project-dir", required=True)
parser.add_argument("--input-schema", default="staging", help="e.g. staging")
parser.add_argument("--output-schema", default="intermediate", help="e.g. intermediate")
args = parser.parse_args()


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


def mk_normalized_dbtmodel(source_name: str, table_name: str) -> str:
    """creates a .sql dbt model"""
    return NORMALIZED_MODEL_TEMPLATE.substitute(
        {"source_name": source_name, "table_name": table_name}
    )


# ================================================================================
def process_source(src: dict, output_schema: str, output_dir: str):
    """iterates through the tables in a source and creates their _normalized models"""
    models = []
    for table in src["tables"]:
        logger.info(
            "[process_source] source_name=%s table_name=%s", src["name"], table["name"]
        )
        table_normalized_dbtmodel = mk_normalized_dbtmodel(src["name"], table["name"])

        table_normalized_dbtmodel_filename = Path(output_dir) / (src["name"] + ".sql")

        logger.info("writing %s", table_normalized_dbtmodel_filename)
        with open(table_normalized_dbtmodel_filename, "w", encoding="utf-8") as outfile:
            outfile.write(table_normalized_dbtmodel)
            outfile.close()

        models.append(
            {
                "name": table["name"],
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
        )

    return models


# ================================================================================
def get_source(filename: str, input_schema: str) -> dict:
    """read the config file containing `sources` keys and return the source
    matching the input schema"""
    with open(filename, "r", encoding="utf-8") as sources_file:
        sources = yaml.safe_load(sources_file)

        for src in sources["sources"]:
            if src["schema"] == input_schema:
                return src

    return None


# ================================================================================
# create the output directory
output_schema_dir = Path(args.project_dir) / "models" / args.output_schema
if not os.path.exists(output_schema_dir):
    os.makedirs(output_schema_dir)
    logger.info("created directory %s", output_schema_dir)

sources_filename = Path(args.project_dir) / "models" / args.input_schema / "sources.yml"
models_filename = Path(args.project_dir) / "models" / args.output_schema / "models.yml"

output_config = {
    "version": 2,
    "models": [],
}

source = get_source(sources_filename, args.input_schema)
if source:
    output_config["models"] = process_source(
        source, args.output_schema, output_schema_dir
    )

    logger.info("writing %s", models_filename)
    with open(models_filename, "w", encoding="utf-8") as models_file:
        yaml.safe_dump(output_config, models_file, sort_keys=False)
