# dbt-automation
Scripts for dbt automation

Example usage:

1. 

    python scaffolddbt.py --project-name shridbt \
                          --project-dir ../shridbt
2. 

    python syncsources.py --schema staging \
                          --source-name shrikobo \
                          --output-sources-file ../shridbt/models/staging/sources.yml

3. 

    python mknormalized.py  --project-dir ../shridbt/ \
                            --input-schema staging \
                            --output-schema intermediate \
                            --output-schema-schemafile schema.yml \
                            --sources-file sources.yml


