from setuptools import setup, find_packages

long_description = ""
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="dbt_automation",
    packages=find_packages(),
    version=0.1,
    author="Dalgo",
    author_email="support@dalgo.in",
    description="This package helps you automate your transformation by generating dbt code",
    license="MIT",
    url="https://github.com/DalgoT4D/dbt-automation",
    project_urls={
        "Documentation": "https://github.com/DalgoT4D/dbt-automation",
        "Source": "https://github.com/DalgoT4D/dbt-automation",
        "Tracker": "https://github.com/DalgoT4D/dbt-automation/issues",
    },
    long_description=long_description,
    python_requires=">=3.7",
    install_requires=["PyYAML", "requests", "google-cloud-bigquery", "psycopg2-binary"],
)
