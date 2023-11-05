import pytest
import os
from pathlib import Path
import subprocess, sys
from logging import basicConfig, getLogger, INFO
from dbt_automation.operations.droprenamecolumns import rename_columns, drop_columns
from dbt_automation.utils.warehouseclient import get_client
from dbt_automation.operations.scaffold import scaffold
from dbt_automation.operations.syncsources import sync_sources
from dbt_automation.operations.flattenairbyte import flatten_operation
from dbt_automation.operations.coalescecolumns import coalesce_columns
from dbt_automation.operations.concatcolumns import concat_columns
from dbt_automation.utils.columnutils import quote_columnname

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

    @staticmethod
    def execute_dbt(cmd: str, select_model: str = None):
        select_cli = ["--select", select_model] if select_model is not None else []
        subprocess.call(
            [
                Path(TestPostgresOperations.test_project_dir) / "venv" / "bin" / "dbt",
                cmd,
            ]
            + select_cli
            + [
                "--project-dir",
                TestPostgresOperations.test_project_dir,
                "--profiles-dir",
                TestPostgresOperations.test_project_dir,
            ],
        )

    def test_scaffold(self, tmpdir):
        """This will setup the dbt repo to run dbt commands after running a test operation"""
        logger.info("starting scaffolding")
        project_name = "pytest_dbt"
        config = {
            "project_name": project_name,
            "default_schema": TestPostgresOperations.schema,
        }
        scaffold(config, TestPostgresOperations.wc_client, tmpdir)
        TestPostgresOperations.test_project_dir = Path(tmpdir) / project_name
        TestPostgresOperations.execute_dbt("deps")
        logger.info("finished scaffolding")

    def test_syncsources(self):
        """test the sync sources operation against the warehouse"""
        logger.info("syncing sources")
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
        logger.info("finished syncing sources")

    def test_flatten(self):
        """test the flatten operation against the warehouse"""
        wc_client = TestPostgresOperations.wc_client
        config = {
            "source_schema": TestPostgresOperations.schema,
            "dest_schema": "pytest_intermediate",
        }
        flatten_operation(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )
        TestPostgresOperations.execute_dbt("run", "Sheet1")
        logger.info("inside test flatten")
        logger.info(
            f"inside project directory : {TestPostgresOperations.test_project_dir}"
        )
        assert "Sheet1" in TestPostgresOperations.wc_client.get_tables(
            "pytest_intermediate"
        )

    def test_rename_columns(self):
        """test rename_columns for sample seed data"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "rename"
        config = {
            "input_name": "Sheet1",
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "columns": {"NGO": "ngo", "Month": "month"},
        }

        rename_columns(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "ngo" in cols
        assert "month" in cols
        assert "NGO" not in cols
        assert "MONTH" not in cols

    def test_drop_columns(self):
        """test drop_columns"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "drop"

        config = {
            "input_name": "Sheet1",
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "columns": ["MONTH"],
        }

        drop_columns(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "MONTH" not in cols

    def test_coalescecolumns(self):
        """test coalescecolumns"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "coalesce"

        config = {
            "input_name": "Sheet1",
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "columns": [
                {
                    "columnname": "NGO",
                },
                {
                    "columnname": "SPOC",
                },
            ],
            "output_column_name": "ngo_spoc",
        }

        coalesce_columns(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "ngo_spoc" in cols
        col_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        col_data_original = wc_client.get_table_data(
            "pytest_intermediate", quote_columnname("Sheet1", "postgres"), 1
        )
        assert (
            col_data[0]["ngo_spoc"] == col_data_original[0]["NGO"]
            if col_data_original[0]["NGO"] is not None
            else col_data_original[0]["SPOC"]
        )

    def test_concatcolumns(self):
        """test concatcolumns"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "concat"

        config = {
            "input_name": "Sheet1",
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "columns": [
                {
                    "name": "NGO",
                    "is_col": "yes",
                },
                {
                    "name": "SPOC",
                    "is_col": "yes",
                },
                {
                    "name": "test",
                    "is_col": "no",
                },
            ],
            "output_column_name": "concat_col",
        }

        concat_columns(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "concat_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert (
            table_data[0]["concat_col"]
            == table_data[0]["NGO"] + table_data[0]["SPOC"] + "test"
        )
