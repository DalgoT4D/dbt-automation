import pytest
import os
from pathlib import Path
import subprocess, sys
from logging import basicConfig, getLogger, INFO
from dbt_automation.operations.droprenamecolumns import rename_columns
from dbt_automation.utils.warehouseclient import get_client
from dbt_automation.operations.scaffold import scaffold
from dbt_automation.operations.syncsources import sync_sources
from dbt_automation.operations.flattenairbyte import flatten_operation


basicConfig(level=INFO)
logger = getLogger()


class TestPostgresOperations:
    """test rename_columns operation"""

    warehouse = "postgres"
    test_project_dir = None
    wc_client = get_client(
        "postgres",
        {
            "host": os.environ.get("TEST_PG_DBHOST"),
            "port": os.environ.get("TEST_PG_DBPORT"),
            "username": os.environ.get("TEST_PG_DBUSER"),
            "database": os.environ.get("TEST_PG_DBNAME"),
            "password": os.environ.get("TEST_PG_DBPASSWORD"),
        },
    )
    schema = os.environ.get(
        "TEST_PG_DBSCHEMA_SRC"
    )  # source schema where the raw data lies

    def test_scaffold(self, tmpdir):
        """This will setup the dbt repo to run dbt commands after running a test operation"""
        print("starting scaffolding")
        project_name = "pytest_dbt"
        config = {
            "project_name": project_name,
            "default_schema": TestPostgresOperations.schema,
        }
        scaffold(config, TestPostgresOperations.wc_client, tmpdir)
        TestPostgresOperations.test_project_dir = Path(tmpdir) / project_name
        subprocess.call(
            [
                Path(TestPostgresOperations.test_project_dir) / "venv" / "bin" / "dbt",
                "deps",
                "--project-dir",
                TestPostgresOperations.test_project_dir,
                "--profiles-dir",
                TestPostgresOperations.test_project_dir,
            ],
        )
        print("finished scaffolding")

    def test_syncsources(self):
        """test the sync sources operation against the warehouse"""
        print("syncing sources")
        config = {
            "source_name": "sample",
            "source_schema": TestPostgresOperations.schema,
        }
        sync_sources(
            config,
            TestPostgresOperations.wc_client,
            TestPostgresOperations.test_project_dir,
        )
        sources_yml = (
            Path(TestPostgresOperations.test_project_dir)
            / "models"
            / TestPostgresOperations.schema
            / "sources.yml"
        )
        assert os.path.exists(sources_yml) is True
        print("finished syncing sources")

    def test_flatten(self):
        """test the flatten operation against the warehouse"""
        config = {
            "source_schema": TestPostgresOperations.schema,
            "dest_schema": "pytest_intermediate",
        }
        flatten_operation(
            config,
            TestPostgresOperations.wc_client,
            TestPostgresOperations.test_project_dir,
        )
        subprocess.call(
            [
                Path(TestPostgresOperations.test_project_dir) / "venv" / "bin" / "dbt",
                "run",
                "--project-dir",
                TestPostgresOperations.test_project_dir,
                "--profiles-dir",
                TestPostgresOperations.test_project_dir,
            ],
        )
        print("inside test flatten")
        print(f"inside project directory : {TestPostgresOperations.test_project_dir}")

    # def test_rename_columns(self):
    #     """test rename_columns for sample seed data"""
    #     config = {"input_name": "", "dest_schema": "", "output_name": "", "columns": {}}

    #     rename_columns(
    #         config,
    #         TestPostgresOperations.wc_client,
    #         TestPostgresOperations.test_project_dir,
    #     )

    #     subprocess.call(
    #         [
    #             "cd",
    #             self.test_project_dir,
    #             "&&",
    #             Path(self.test_project_dir) / "venv" / "bin" / "dbt",
    #             "debug",
    #         ],
    #     )
