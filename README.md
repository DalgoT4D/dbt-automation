# dbt-automation

Documentation can be found [here](https://github.com/DalgoT4D/dbt-automation/wiki)

# Setting up the test environment

- Create a `pytest.ini` file and the test warehouse credentials. 
- Create a `dbconnection.env` and add the test warehouse credentials. The test warehouse credentials will be used to seed data
- Seed the sample data by running ```python seeds/seed.py --warehouse <postgres or bigquery>```
- Run pytest ```pytest tests/ -c pytest.ini -s``` in your local virtual environment
