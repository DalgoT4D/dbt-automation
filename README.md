# dbt-automation

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Code coverage badge](https://img.shields.io/codecov/c/github/DalgoT4D/dbt-automation/main.svg)](https://codecov.io/gh/DalgoT4D/dbt-automation/branch/main)

Documentation can be found [here](https://github.com/DalgoT4D/dbt-automation/wiki)

# NOTE the contents of this repository have been copied over to https://github.com/DalgoT4D/DDP_backend and the repository has been archived


# Setting up the test environment

- Create a `pytest.ini` file and the test warehouse credentials. 
- Create a `dbconnection.env` and add the test warehouse credentials. The test warehouse credentials will be used to seed data
- Seed the sample data by running ```python seeds/seed.py --warehouse <postgres or bigquery>```
- Run pytest ```pytest tests/ -c pytest.ini -s``` in your local virtual environment
