# dbt-automation
Scripts for dbt automation

Example usage:

1. 

    python scaffolddbt.py --project-dir workspace/lahi \
                          --project-name lahi 
2. 

    python syncsources.py --project-dir workspace/lahi \
                          --source-name lahi 

3. 

    python flattenairbyte.py  --project-dir workspace/lahi


# Setting up the test environment

- Create a `pytest.ini` file and the test warehouse credentials. 
- Create a `dbconnection.env` and add the test warehouse credentials. The test warehouse credentials will be used to seed data
- Seed the sample data by running ```python seeds/seed.py --warehouse <postgres or bigquery>```
- Run pytest ```pytest tests/ -c pytest.ini -s``` in your local virtual environment
