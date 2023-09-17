"""creates the dbt models which call our flatten_json macro"""
import sys
import argparse
from logging import basicConfig, getLogger, INFO
from string import Template
from pathlib import Path
import yaml
from lib.sourceschemas import get_source
from lib.dbtproject import dbtProject

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
def write_models(src: dict, output_dir: str):
    """iterates through the tables in a source and creates their normalized models"""
    for table in src["tables"]:
        model_filename = Path(output_dir) / (table["name"] + ".sql")
        logger.info(
            "[write_models] %s.%s => %s", src["name"], table["name"], model_filename
        )

        with open(model_filename, "w", encoding="utf-8") as outfile:
            model = mk_normalized_dbtmodel(src["name"], table["name"])
            outfile.write(model)
            outfile.close()


# ================================================================================
def get_models_config(src: dict, output_schema: str):
    """iterates through the tables in a source and creates the corresponding model
    configs. only one column is specified in the model: _airbyte_ab_id"""
    models = []
    for table in src["tables"]:
        logger.info(
            "[get_models_config] adding model %s.%s",
            src["name"],
            table["name"],
        )

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
dbtproject = dbtProject(args.project_dir)

# create the output directory
dbtproject.ensure_models_dir(args.output_schema)

# locate the sources.yml for the input-schema
sources_filename = dbtproject.sources_filename(args.input_schema)

# find the source in that file... it should be the only one
source = get_source(sources_filename, args.input_schema)
if source is None:
    logger.error("no source for schema %s in %s", args.input_schema, sources_filename)
    sys.exit(1)

# for every table in the source, generate an output model file
output_schema_dir = dbtproject.models_dir(args.output_schema)
write_models(source, output_schema_dir)

# also generate a configuration yml with a `models:` key under the output-schema
models_filename = dbtproject.models_filename(args.output_schema)
with open(models_filename, "w", encoding="utf-8") as models_file:
    logger.info("writing %s", models_filename)
    yaml.safe_dump(
        {
            "version": 2,
            "models": get_models_config(source, args.output_schema),
        },
        models_file,
        sort_keys=False,
    )
