"""reads tables from db to create a dbt sources.yml"""
import os
import argparse
from logging import basicConfig, getLogger, INFO
import yaml
import psycopg2

from dotenv import load_dotenv

load_dotenv("syncsources.env")

basicConfig(level=INFO)
logger = getLogger()

parser = argparse.ArgumentParser(
    """
Generates a source.yml containing exactly one source
That source will have one or more tables
"""
)
parser.add_argument("--schema", required=True, help="e.g. staging")
parser.add_argument("--source-name", required=True)
parser.add_argument("--input-sources-file")
parser.add_argument(
    "--output-sources-file",
    help="can be the same as input-sources-file, will overwrite",
)
args = parser.parse_args()


# ================================================================================
def readsourcedefinitions(sourcefilename: str):
    """read the source definitions from a dbt sources.yml"""
    with open(sourcefilename, "r", encoding="utf-8") as sourcefile:
        sourcedefinitions = yaml.safe_load(sourcefile)
        return sourcedefinitions


# ================================================================================
def mksourcedefinition(sourcename: str, input_schema: str, tables: list):
    """generates the data structure for a dbt sources.yml"""
    airbyte_prefix = "_airbyte_raw_"

    source = {"name": sourcename, "schema": input_schema, "tables": []}

    for tablename in tables:
        cleaned_name = tablename.replace(airbyte_prefix, "")
        source["tables"].append(
            {
                "name": cleaned_name,
                "identifier": tablename,
                "description": "",
            }
        )

    sourcedefinitions = {
        "version": 2,
        "sources": [source],
    }
    return sourcedefinitions


# ================================================================================
def mergesource(dbsource: dict, filesources: list) -> dict:
    """
    finds the file source corresponding to the dbsource
    and update the name if possible
    """
    outputsource = {
        "name": dbsource["name"],
        "schema": dbsource["schema"],
        "tables": [],
    }

    for filesource in filesources:
        if outputsource["schema"] == filesource["schema"]:
            outputsource["name"] = filesource["name"]
            outputsource["tables"] = [
                mergetable(dbtable, filesource["tables"])
                for dbtable in dbsource["tables"]
            ]
            return outputsource

    # if we didn't find anything in the file source, then
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
def make_source_definitions(
    source_name: str,
    input_schema: str,
    existing_source_definitions_file=None,
    output_sources_file=None,
):
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

    if existing_source_definitions_file:
        filesourcedefinitions = readsourcedefinitions(existing_source_definitions_file)
        logger.info(
            "read existing source defs from %s", existing_source_definitions_file
        )

    else:
        filesourcedefinitions = {"version": 2, "sources": []}

    merged_definitions = merge_sourcedefinitions(
        filesourcedefinitions, dbsourcedefinitions
    )
    logger.info("created (new) source definitions")
    if output_sources_file:
        with open(output_sources_file, "w", encoding="utf-8") as outfile:
            yaml.safe_dump(merged_definitions, outfile, sort_keys=False)
            logger.info("wrote source definitions to %s", output_sources_file)
    else:
        logger.info("sources to be written to file:")
        logger.info(merged_definitions)


# ================================================================================
if __name__ == "__main__":
    make_source_definitions(
        args.source_name, args.schema, args.input_sources_file, args.output_sources_file
    )
