"""reads tables from db to create a dbt sources.yml"""
import os
import argparse
from logging import basicConfig, getLogger, INFO
from pathlib import Path
import yaml
import psycopg2
from dotenv import load_dotenv
from lib.sourceschemas import mksourcedefinition


load_dotenv("syncsources.env")

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser(
    """
Generates a sources.yml configuration containing exactly one source
That source will have one or more tables
Ref: https://docs.getdbt.com/reference/source-properties
Database connection parameters are read from syncsources.env
"""
)
parser.add_argument("--project-dir", required=True)
parser.add_argument("--source-name", required=True)
parser.add_argument("--schema", default="staging", help="e.g. staging")
args = parser.parse_args()


# ================================================================================
def readsourcedefinitions(sourcefilename: str):
    """read the source definitions from a dbt sources.yml"""
    with open(sourcefilename, "r", encoding="utf-8") as sourcefile:
        sourcedefinitions = yaml.safe_load(sourcefile)
        return sourcedefinitions


# ================================================================================
def mergesource(dbsource: dict, filesources: list) -> dict:
    """
    finds the file source corresponding to the dbsource
    and update the name if possible
    """
    outputsource = {
        "name": None,
        "schema": dbsource["schema"],
        "tables": None,
    }

    try:
        filesource = next(
            fs for fs in filesources if fs["schema"] == dbsource["schema"]
        )
        outputsource["name"] = filesource["name"]
        outputsource["tables"] = [
            mergetable(dbtable, filesource["tables"]) for dbtable in dbsource["tables"]
        ]
    except StopIteration:
        outputsource["name"] = dbsource["name"]
        outputsource["tables"] = dbsource["tables"]

    return outputsource


# ================================================================================
def mergetable(dbtable: dict, filetables: list):
    """
    finds the dbtable in the list of filetables by `identifier`
    copies over the name and description if found
    """
    outputtable = {
        "name": dbtable["identifier"],
        "identifier": dbtable["identifier"],
        "description": "",
    }
    for filetable in filetables:
        if outputtable["identifier"] == filetable["identifier"]:
            outputtable["name"] = filetable["name"]
            outputtable["description"] = filetable["description"]
            break
    return outputtable


# ================================================================================
def merge_sourcedefinitions(filedefs: dict, dbdefs: dict) -> dict:
    """outputs source definitions from dbdefs, with the descriptions from filedefs"""
    outputdefs = {}
    outputdefs["version"] = filedefs["version"]
    outputdefs["sources"] = [
        mergesource(dbsource, filedefs["sources"]) for dbsource in dbdefs["sources"]
    ]

    return outputdefs


# ================================================================================
def get_connection():
    """returns a psycopg connection"""
    connection = psycopg2.connect(
        host=os.getenv("DBHOST"),
        port=os.getenv("DBPORT"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBNAME"),
    )
    return connection


# ================================================================================
def make_source_definitions(source_name: str, input_schema: str, sources_file: str):
    """
    reads tables from the input_schema to create a dbt sources.yml
    uses the metadata from the existing source definitions, if any
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{input_schema}'
        """
        )
        resultset = cursor.fetchall()
        tablenames = [x[0] for x in resultset]

        dbsourcedefinitions = mksourcedefinition(source_name, input_schema, tablenames)
        logger.info("read sources from database schema %s", input_schema)

    if Path(sources_file).exists():
        filesourcedefinitions = readsourcedefinitions(sources_file)
        logger.info("read existing source defs from %s", sources_file)

    else:
        filesourcedefinitions = {"version": 2, "sources": []}

    merged_definitions = merge_sourcedefinitions(
        filesourcedefinitions, dbsourcedefinitions
    )
    logger.info("created (new) source definitions")
    with open(sources_file, "w", encoding="utf-8") as outfile:
        yaml.safe_dump(merged_definitions, outfile, sort_keys=False)
        logger.info("wrote source definitions to %s", sources_file)


# ================================================================================
if __name__ == "__main__":
    sources_filename = Path(args.project_dir) / "models" / args.schema / "sources.yml"
    make_source_definitions(args.source_name, args.schema, sources_filename)
