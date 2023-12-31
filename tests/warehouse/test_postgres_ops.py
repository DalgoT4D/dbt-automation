import pytest
import os
from pathlib import Path
import math
import subprocess, sys
from logging import basicConfig, getLogger, INFO
from dbt_automation.operations.droprenamecolumns import rename_columns, drop_columns
from dbt_automation.utils.warehouseclient import get_client
from dbt_automation.operations.scaffold import scaffold
from dbt_automation.operations.syncsources import sync_sources
from dbt_automation.operations.flattenairbyte import flatten_operation
from dbt_automation.operations.coalescecolumns import coalesce_columns
from dbt_automation.operations.concatcolumns import concat_columns
from dbt_automation.operations.arithmetic import arithmetic
from dbt_automation.operations.castdatatypes import cast_datatypes
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.operations.regexextraction import regex_extraction
from dbt_automation.operations.mergetables import union_tables


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
        try:
            select_cli = ["--select", select_model] if select_model is not None else []
            subprocess.check_call(
                [
                    Path(TestPostgresOperations.test_project_dir)
                    / "venv"
                    / "bin"
                    / "dbt",
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
        except subprocess.CalledProcessError as e:
            logger.error(f"dbt {cmd} failed with {e.returncode}")
            raise Exception(f"Something went wrong while running dbt {cmd}")

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
        TestPostgresOperations.execute_dbt("run", "Sheet2")
        logger.info("inside test flatten")
        logger.info(
            f"inside project directory : {TestPostgresOperations.test_project_dir}"
        )
        assert "Sheet1" in TestPostgresOperations.wc_client.get_tables(
            "pytest_intermediate"
        )
        assert "Sheet2" in TestPostgresOperations.wc_client.get_tables(
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

    def test_castdatatypes(self):
        """test castdatatypes"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "cast"

        config = {
            "input_name": "Sheet1",
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "columns": [
                {
                    "columnname": "measure1",
                    "columntype": "int",
                },
                {
                    "columnname": "measure2",
                    "columntype": "int",
                },
            ],
        }

        cast_datatypes(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "measure1" in cols
        assert "measure2" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        # TODO: do stronger check here; fetch datatype from warehouse and then compare/assert
        assert type(table_data[0]["measure1"]) == int
        assert type(table_data[0]["measure2"]) == int

    def test_arithmetic_add(self):
        """test arithmetic addition"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "arithmetic_add"

        config = {
            "input_name": "cast",  # from previous operation
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "operator": "add",
            "operands": ["measure1", "measure2"],
            "output_column_name": "add_col",
        }

        arithmetic(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "add_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert (
            table_data[0]["add_col"]
            == table_data[0]["measure1"] + table_data[0]["measure2"]
        )

    def test_arithmetic_sub(self):
        """test arithmetic subtraction"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "arithmetic_sub"

        config = {
            "input_name": "cast",  # from previous operation
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "operator": "sub",
            "operands": ["measure1", "measure2"],
            "output_column_name": "sub_col",
        }

        arithmetic(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "sub_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert (
            table_data[0]["sub_col"]
            == table_data[0]["measure1"] - table_data[0]["measure2"]
        )

    def test_arithmetic_mul(self):
        """test arithmetic multiplication"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "arithmetic_mul"

        config = {
            "input_name": "cast",  # from previous operation
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "operator": "mul",
            "operands": ["measure1", "measure2"],
            "output_column_name": "mul_col",
        }

        arithmetic(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "mul_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert (
            table_data[0]["mul_col"]
            == table_data[0]["measure1"] * table_data[0]["measure2"]
        )

    def test_arithmetic_div(self):
        """test arithmetic division"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "arithmetic_div"

        config = {
            "input_name": "cast",  # from previous operation
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "operator": "div",
            "operands": ["measure1", "measure2"],
            "output_column_name": "div_col",
        }

        arithmetic(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "div_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert (
            math.ceil(table_data[0]["measure1"] / table_data[0]["measure2"])
            if table_data[0]["measure2"] != 0
            else None
            == (
                math.ceil(table_data[0]["div_col"])
                if table_data[0]["div_col"] is not None
                else None
            )
        )

    def test_regexextract(self):
        """test regex extraction"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "regex_ext"

        config = {
            "input_name": "Sheet1",
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "columns": {"NGO": "^[C].*"},
        }

        regex_extraction(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "NGO" in cols
        table_data_org = wc_client.get_table_data(
            "pytest_intermediate", quote_columnname("Sheet1", "postgres"), 10
        )
        table_data_org.sort(key=lambda x: x["_airbyte_ab_id"])
        table_data_regex = wc_client.get_table_data(
            "pytest_intermediate", output_name, 10
        )
        table_data_regex.sort(key=lambda x: x["_airbyte_ab_id"])
        for regex, org in zip(table_data_regex, table_data_org):
            assert (
                regex["NGO"] == org["NGO"]
                if org["NGO"].startswith("C")
                else (regex["NGO"] is None)
            )

    def test_mergetables(self):
        """test merge tables"""
        wc_client = TestPostgresOperations.wc_client
        output_name = "union"

        config = {
            "dest_schema": "pytest_intermediate",
            "output_name": output_name,
            "tablenames": ["Sheet1", "Sheet2"],
        }

        union_tables(
            config,
            wc_client,
            TestPostgresOperations.test_project_dir,
        )

        TestPostgresOperations.execute_dbt("run", output_name)

        table_data1 = wc_client.get_table_data(
            "pytest_intermediate", quote_columnname("Sheet1", "postgres"), 10
        )
        table_data2 = wc_client.get_table_data(
            "pytest_intermediate", quote_columnname("Sheet2", "postgres"), 10
        )
        table_data_union = wc_client.get_table_data(
            "pytest_intermediate", output_name, 10
        )

        assert len(table_data1) + len(table_data2) == len(table_data_union)
