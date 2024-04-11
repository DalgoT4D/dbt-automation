import argparse
import json

from logging import basicConfig, getLogger, INFO
import yaml

from dbt_automation.utils.postgres import PostgresClient

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--config-yaml", default="connections.yaml")
parser.add_argument("--tableroot", required=True)
parser.add_argument("--id", required=True)
args = parser.parse_args()

with open(args.config_yaml, "r", encoding="utf-8") as connection_yaml:
    connection_info = yaml.safe_load(connection_yaml)

ref_client = PostgresClient(connection_info["ref"])
comp_client = PostgresClient(connection_info["comp"])

ref_table = f"_airbyte_raw_{args.tableroot}".lower()
comp_table = f"airbyte_{args.tableroot}"

ref_cmd = f"SELECT _airbyte_data->>'data' FROM staging.\"{ref_table}\" WHERE _airbyte_data->>'id' = '{args.id}'"
ref_json = ref_client.execute(ref_cmd)

comp_cmd = f"SELECT data FROM staging.\"{comp_table}\" WHERE data->>'id' = '{args.id}'"
comp_json = comp_client.execute(comp_cmd)

print(json.loads(ref_json[0][0]))
# print(json.dumps(comp_json[0][0], indent=2))
