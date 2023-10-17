import yaml


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
