"""the dbt project structure"""

import os
from pathlib import Path


class dbtProject:  # pylint:disable=invalid-name
    """the folder and files in a dbt project"""

    def __init__(self, project_dir: str):
        """constructor"""
        self.project_dir = project_dir

    def sources_filename(self, schema: str) -> str:
        """returns the pathname of the sources.yml in the folder for the given schema"""
        return Path(self.project_dir) / "models" / schema / "sources.yml"

    def models_dir(self, schema: str) -> str:
        """returns the path of the models folder for the given schema"""
        return Path(self.project_dir) / "models" / schema

    def ensure_models_dir(self, schema: str) -> None:
        """ensures the existence of the output models folder for the given schema"""
        output_schema_dir = self.models_dir(schema)
        if not os.path.exists(output_schema_dir):
            os.makedirs(output_schema_dir)

    def models_filename(self, schema: str) -> str:
        """returns the pathname of the models.yml in the folder for the given schema"""
        return Path(self.models_dir(schema)) / "models.yml"
