import pytest
import os
from pathlib import Path
import math
import json
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


basicConfig(level=INFO)
logger = getLogger()


class TestBigqueryOperations:
    """test operations in bigquery warehouse"""

    warehouse = "bigquery"
    test_project_dir = None

    wc_client = get_client(
        "bigquery",
        json.loads(os.getenv("TEST_BG_SERVICEJSON")),
        os.environ.get("TEST_BG_LOCATION"),
    )
    schema = os.environ.get(
        "TEST_BG_DATASET_SRC"
    )  # source schema where the raw data lies

    @staticmethod
    def execute_dbt(cmd: str, select_model: str = None):
        try:
            select_cli = ["--select", select_model] if select_model is not None else []
            subprocess.check_call(
                [
                    Path(TestBigqueryOperations.test_project_dir)
                    / "venv"
                    / "bin"
                    / "dbt",
                    cmd,
                ]
                + select_cli
                + [
                    "--project-dir",
                    TestBigqueryOperations.test_project_dir,
                    "--profiles-dir",
                    TestBigqueryOperations.test_project_dir,
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
            "default_schema": TestBigqueryOperations.schema,
        }
        scaffold(config, TestBigqueryOperations.wc_client, tmpdir)
        TestBigqueryOperations.test_project_dir = Path(tmpdir) / project_name
        TestBigqueryOperations.execute_dbt("deps")
        logger.info("finished scaffolding")

    def test_syncsources(self):
        """test the sync sources operation against the warehouse"""
        logger.info("syncing sources")
        config = {
            "source_name": "sample",
            "source_schema": TestBigqueryOperations.schema,
        }
        sync_sources(
            config,
            TestBigqueryOperations.wc_client,
            TestBigqueryOperations.test_project_dir,
        )
        sources_yml = (
            Path(TestBigqueryOperations.test_project_dir)
            / "models"
            / TestBigqueryOperations.schema
            / "sources.yml"
        )
        assert os.path.exists(sources_yml) is True
        logger.info("finished syncing sources")

    def test_flatten(self):
        """test the flatten operation against the warehouse"""
        wc_client = TestBigqueryOperations.wc_client
        config = {
            "source_schema": TestBigqueryOperations.schema,
            "dest_schema": "pytest_intermediate",
        }
        flatten_operation(
            config,
            wc_client,
            TestBigqueryOperations.test_project_dir,
        )
        TestBigqueryOperations.execute_dbt("run", "Sheet1")
        logger.info("inside test flatten")
        logger.info(
            f"inside project directory : {TestBigqueryOperations.test_project_dir}"
        )
        assert "Sheet1" in TestBigqueryOperations.wc_client.get_tables(
            "pytest_intermediate"
        )

    # def test_rename_columns(self):
    #     """test rename_columns for sample seed data"""
    #     wc_client = TestBigqueryOperations.wc_client
    #     output_name = "rename"
    #     config = {
    #         "input_name": "Sheet1",
    #         "dest_schema": "pytest_intermediate",
    #         "output_name": output_name,
    #         "columns": {"NGO": "ngo", "Month": "month"},
    #     }

    #     rename_columns(
    #         config,
    #         wc_client,
    #         TestBigqueryOperations.test_project_dir,
    #     )

    #     TestBigqueryOperations.execute_dbt("run", output_name)

    #     cols = wc_client.get_table_columns("pytest_intermediate", output_name)
    #     assert "ngo" in cols
    #     assert "month" in cols
    #     assert "NGO" not in cols
    #     assert "MONTH" not in cols

    # def test_drop_columns(self):
    #     """test drop_columns"""
    #     wc_client = TestBigqueryOperations.wc_client
    #     output_name = "drop"

    #     config = {
    #         "input_name": "Sheet1",
    #         "dest_schema": "pytest_intermediate",
    #         "output_name": output_name,
    #         "columns": ["MONTH"],
    #     }

    #     drop_columns(
    #         config,
    #         wc_client,
    #         TestBigqueryOperations.test_project_dir,
    #     )

    #     TestBigqueryOperations.execute_dbt("run", output_name)

    #     cols = wc_client.get_table_columns("pytest_intermediate", output_name)
    #     assert "MONTH" not in cols

    # def test_coalescecolumns(self):
    #     """test coalescecolumns"""
    #     wc_client = TestBigqueryOperations.wc_client
    #     output_name = "coalesce"

    #     config = {
    #         "input_name": "Sheet1",
    #         "dest_schema": "pytest_intermediate",
    #         "output_name": output_name,
    #         "columns": [
    #             {
    #                 "columnname": "NGO",
    #             },
    #             {
    #                 "columnname": "SPOC",
    #             },
    #         ],
    #         "output_column_name": "ngo_spoc",
    #     }

    #     coalesce_columns(
    #         config,
    #         wc_client,
    #         TestBigqueryOperations.test_project_dir,
    #     )

    #     TestBigqueryOperations.execute_dbt("run", output_name)

    #     cols = wc_client.get_table_columns("pytest_intermediate", output_name)
    #     assert "ngo_spoc" in cols
    #     col_data = wc_client.get_table_data("pytest_intermediate", output_name, 5)
    #     col_data_original = wc_client.get_table_data("pytest_intermediate", "Sheet1", 5)
    #     col_data_original.sort(key=lambda x: x["_airbyte_ab_id"])
    #     col_data.sort(key=lambda x: x["_airbyte_ab_id"])
    #     # TODO: can do a stronger check here; by checking on rows in a loop
    #     assert (
    #         col_data[0]["ngo_spoc"] == col_data_original[0]["NGO"]
    #         if col_data_original[0]["NGO"] is not None
    #         else col_data_original[0]["SPOC"]
    #     )

    # def test_concatcolumns(self):
    #     """test concatcolumns"""
    #     wc_client = TestBigqueryOperations.wc_client
    #     output_name = "concat"

    #     config = {
    #         "input_name": "Sheet1",
    #         "dest_schema": "pytest_intermediate",
    #         "output_name": output_name,
    #         "columns": [
    #             {
    #                 "name": "NGO",
    #                 "is_col": "yes",
    #             },
    #             {
    #                 "name": "SPOC",
    #                 "is_col": "yes",
    #             },
    #             {
    #                 "name": "test",
    #                 "is_col": "no",
    #             },
    #         ],
    #         "output_column_name": "concat_col",
    #     }

    #     concat_columns(
    #         config,
    #         wc_client,
    #         TestBigqueryOperations.test_project_dir,
    #     )

    #     TestBigqueryOperations.execute_dbt("run", output_name)

    #     cols = wc_client.get_table_columns("pytest_intermediate", output_name)
    #     assert "concat_col" in cols
    #     table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
    #     assert (
    #         table_data[0]["concat_col"]
    #         == table_data[0]["NGO"] + table_data[0]["SPOC"] + "test"
    #     )

    def test_castdatatypes(self):
        """test castdatatypes"""
        wc_client = TestBigqueryOperations.wc_client
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
            TestBigqueryOperations.test_project_dir,
        )

        TestBigqueryOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "measure1" in cols
        assert "measure2" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        # TODO: do stronger check here; fetch datatype from warehouse and then compare/assert
        assert type(table_data[0]["measure1"]) == int
        assert type(table_data[0]["measure2"]) == int

    # def test_arithmetic_add(self):
    #     """test arithmetic addition"""
    #     wc_client = TestBigqueryOperations.wc_client
    #     output_name = "arithmetic_add"

    #     config = {
    #         "input_name": "cast",  # from previous operation
    #         "dest_schema": "pytest_intermediate",
    #         "output_name": output_name,
    #         "operator": "add",
    #         "operands": ["measure1", "measure2"],
    #         "output_column_name": "add_col",
    #     }

    #     arithmetic(
    #         config,
    #         wc_client,
    #         TestBigqueryOperations.test_project_dir,
    #     )

    #     TestBigqueryOperations.execute_dbt("run", output_name)

    #     cols = wc_client.get_table_columns("pytest_intermediate", output_name)
    #     assert "add_col" in cols
    #     table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
    #     assert (
    #         table_data[0]["add_col"]
    #         == table_data[0]["measure1"] + table_data[0]["measure2"]
    #     )

    # def test_arithmetic_sub(self):
    #     """test arithmetic subtraction"""
    #     wc_client = TestBigqueryOperations.wc_client
    #     output_name = "arithmetic_sub"

    #     config = {
    #         "input_name": "cast",  # from previous operation
    #         "dest_schema": "pytest_intermediate",
    #         "output_name": output_name,
    #         "operator": "sub",
    #         "operands": ["measure1", "measure2"],
    #         "output_column_name": "sub_col",
    #     }

    #     arithmetic(
    #         config,
    #         wc_client,
    #         TestBigqueryOperations.test_project_dir,
    #     )

    #     TestBigqueryOperations.execute_dbt("run", output_name)

    #     cols = wc_client.get_table_columns("pytest_intermediate", output_name)
    #     assert "sub_col" in cols
    #     table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
    #     assert (
    #         table_data[0]["sub_col"]
    #         == table_data[0]["measure1"] - table_data[0]["measure2"]
    #     )

    # def test_arithmetic_mul(self):
    #     """test arithmetic multiplication"""
    #     wc_client = TestBigqueryOperations.wc_client
    #     output_name = "arithmetic_mul"

    #     config = {
    #         "input_name": "cast",  # from previous operation
    #         "dest_schema": "pytest_intermediate",
    #         "output_name": output_name,
    #         "operator": "mul",
    #         "operands": ["measure1", "measure2"],
    #         "output_column_name": "mul_col",
    #     }

    #     arithmetic(
    #         config,
    #         wc_client,
    #         TestBigqueryOperations.test_project_dir,
    #     )

    #     TestBigqueryOperations.execute_dbt("run", output_name)

    #     cols = wc_client.get_table_columns("pytest_intermediate", output_name)
    #     assert "mul_col" in cols
    #     table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
    #     assert (
    #         table_data[0]["mul_col"]
    #         == table_data[0]["measure1"] * table_data[0]["measure2"]
    #     )

    def test_arithmetic_div(self):
        """test arithmetic division"""
        wc_client = TestBigqueryOperations.wc_client
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
            TestBigqueryOperations.test_project_dir,
        )

        TestBigqueryOperations.execute_dbt("run", output_name)

        cols = wc_client.get_table_columns("pytest_intermediate", output_name)
        assert "div_col" in cols
        table_data = wc_client.get_table_data("pytest_intermediate", output_name, 1)
        assert (
            math.ceil(table_data[0]["div_col"])
            == math.ceil(table_data[0]["measure1"] / table_data[0]["measure2"])
            if table_data[0]["measure2"] != 0
            else None
        )
