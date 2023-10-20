"""
This file does basic airthmetic column operations

Input: 
    - warehouse
    - input schema
    - output schema
    - operations: array
        - operation : '+', '*', '-', '/'
        - operand 1 : string (column name)
        - operand 2 : string (column name) or numeric (to scale)
    - name of the output table

1. Add/Subtract two columns and create a new one

2. Multiply two columns and create a new one

3. Scale (+-*/) a column with constant

"""


import os
import argparse
from collections import defaultdict
from logging import basicConfig, getLogger, INFO
from dotenv import load_dotenv
import yaml
import pandas as pd

# from lib.dbtproject import dbtProject
from lib.postgres import PostgresClient
from lib.bigquery import BigQueryClient

from lib.dbtproject import dbtProject

basicConfig(level=INFO)
logger = getLogger()


# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--output-schema", required=True)
parser.add_argument("--input-model", required=True)
parser.add_argument("--name", required=True)
args = parser.parse_args()

# ================================================================================
load_dotenv("dbconnection.env")
project_dir = os.getenv("DBT_PROJECT_DIR")

# 1. read/take the input model name


# 2. read an operation - operand 1 (dbt model col or numeric), operand 2 (dbt model col or numeric), operation


# 3. generate dbt model config that does the operation, basically use write_model in the output defined schema
