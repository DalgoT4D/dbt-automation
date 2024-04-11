import os
import sys
import json
from pathlib import Path
from glob import glob
import argparse
from logging import basicConfig, getLogger, INFO
import yaml

from dbt_automation.utils.postgres import PostgresClient

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--config-yaml", default="connections.yaml")
parser.add_argument("--working-dir", required=True)
parser.add_argument("--tableroot")
args = parser.parse_args()

with open(args.config_yaml, "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

# ref_client = PostgresClient(connection_info["ref"])
comp_client = PostgresClient(connection_info["comp"])

id_files = glob(args.working_dir + "/*.csv")

for id_file in id_files:
    if id_file[-len("_comp.csv") :] == "_comp.csv":
        filename = Path(id_file).name
        tableroot = filename[: -len("_comp.csv")]
        if args.tableroot and tableroot != args.tableroot:
            continue
        with open(id_file, "r", encoding="utf-8") as f:
            for line in f:
                the_id = line.strip()
                the_tablename = "airbyte_" + tableroot
                # print(the_tablename, the_id)
                select_cmd = f"SELECT * FROM staging.\"{the_tablename}\" WHERE data ->> 'id' = '{the_id}';"
                # print(select_cmd)
                for rows in comp_client.execute(select_cmd):
                    print(rows)
                delete_cmd = f"DELETE FROM staging.\"{the_tablename}\" WHERE data ->> 'id' = '{the_id}';"
                comp_client.runcmd(delete_cmd)
