"""creates the dbt project"""
import os
import sys
import shutil
from string import Template
import argparse
from pathlib import Path
from logging import basicConfig, getLogger, INFO
import yaml

basicConfig(level=INFO)
logger = getLogger()

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--project-dir", required=True)
parser.add_argument(
    "--project-name",
    required=True,
    help="a profile of this name must exist in your profiles.yml",
)
args = parser.parse_args()

project_dir = args.project_dir

if os.path.exists(project_dir):
    print("directory exists: %s", project_dir)
    sys.exit(1)

logger.info("mkdir %s", project_dir)
os.makedirs(project_dir)

for subdir in [
    # "analyses",
    "logs",
    "macros",
    "models",
    # "seeds",
    # "snapshots",
    "target",
    "tests",
]:
    (Path(project_dir) / subdir).mkdir()
    logger.info("created %s", str(Path(project_dir) / subdir))

(Path(project_dir) / "models" / "staging").mkdir()
(Path(project_dir) / "models" / "intermediate").mkdir()

flatten_json_target = Path(project_dir) / "macros" / "flatten_json.sql"
custom_schema_target = Path(project_dir) / "macros" / "generate_schema_name.sql"
logger.info("created %s", flatten_json_target)
shutil.copy("dbt_automation/assets/generate_schema_name.sql", custom_schema_target)
logger.info("created %s", custom_schema_target)

dbtproject_filename = Path(project_dir) / "dbt_project.yml"
PROJECT_TEMPLATE = Template(
    """
name: '$project_name'
version: '1.0.0'
config-version: 2
profile: '$project_name'
model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"
"""
)
dbtproject = PROJECT_TEMPLATE.substitute({"project_name": args.project_name})
with open(dbtproject_filename, "w", encoding="utf-8") as dbtprojectfile:
    dbtprojectfile.write(dbtproject)
    logger.info("wrote %s", dbtproject_filename)

dbtpackages_filename = Path(project_dir) / "packages.yml"
with open(dbtpackages_filename, "w", encoding="utf-8") as dbtpackgesfile:
    yaml.safe_dump(
        {"packages": [{"package": "dbt-labs/dbt_utils", "version": "1.1.1"}]},
        dbtpackgesfile,
    )
