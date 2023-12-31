"""the dbt project structure"""

import os
from pathlib import Path
import yaml


class dbtProject:  # pylint:disable=invalid-name
    """the folder and files in a dbt project"""

    def __init__(self, project_dir: str):
        """constructor"""
        self.project_dir = project_dir

    def sources_filename(self, schema: str) -> str:
        """returns the pathname of the sources.yml in the folder for the given schema"""
        return Path(self.project_dir) / "models" / schema / "sources.yml"

    def models_dir(self, schema: str, subdir="") -> str:
        """returns the path of the models folder for the given schema"""
        return Path(self.project_dir) / "models" / schema / subdir

    def ensure_models_dir(self, schema: str, subdir="") -> None:
        """ensures the existence of the output models folder for the given schema"""
        output_schema_dir = self.models_dir(schema, subdir)
        if not os.path.exists(output_schema_dir):
            os.makedirs(output_schema_dir)

    def write_model(
        self, schema: str, modelname: str, model_sql: str, **kwargs
    ) -> None:
        """writes a .sql model"""
        self.ensure_models_dir(schema, kwargs.get("subdir", ""))
        model_sql = (
            "--DBT AUTOMATION has generated this model, please DO NOT EDIT \n--Please make sure you dont change the model name \n\n"
            + model_sql
        )
        model_filename = Path(self.models_dir(schema, kwargs.get("subdir", ""))) / (
            modelname + ".sql"
        )
        with open(model_filename, "w", encoding="utf-8") as outfile:
            if kwargs.get("logger"):
                kwargs["logger"].info("[write_model] %s", model_filename)
            outfile.write(model_sql)
            outfile.close()

    def write_model_config(self, schema: str, models: list, **kwargs) -> None:
        """writes a .yml with a models: key"""
        self.ensure_models_dir(schema, kwargs.get("subdir", ""))
        models_filename = (
            Path(self.models_dir(schema, kwargs.get("subdir", ""))) / "models.yml"
        )
        with open(models_filename, "w", encoding="utf-8") as models_file:
            if kwargs.get("logger"):
                kwargs["logger"].info("writing %s", models_filename)
            yaml.safe_dump(
                {
                    "version": 2,
                    "models": models,
                },
                models_file,
                sort_keys=False,
            )
