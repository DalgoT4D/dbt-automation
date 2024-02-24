## Compare the dbt outputs between the production and local SHRI warehouses

1. Sync on production 

2. Run postgres on ec2 machine via docker: 
    sudo service postgresql stop # in case there is a postgres running on the machine
    docker run --name postgres \
              -e POSTGRES_PASSWORD=mysecretpassword \
              -p 5432:5432 \
              -v /home/ubuntu/postgres_data:/var/lib/postgresql/data \
              -d \
              postgres:14.9-alpine3.18

  Then create the database and user for airbyte using psql

3. We need to access this database from within the airbyte container, and "host.docker.internal" doesn't work on linux
   To make "dockerhost" available from within the container, run:
     docker run --add-host dockerhost:`docker network inspect --format='{{range .IPAM.Config}}{{.Gateway}}{{end}}' bridge` 

4. Sync kobo to the local postgres running outside airbyte

5. Sync the calculations and _airbyte_raw_calculations tables using pg_dump on the sanrights database and psql on the local db

6. Remove rows which don't exist in production i.e. which were created between the two syncs. 
    create the connections.yaml required by and run
      python trimtosyncairbyteraw.py 


7. Run dbt_shri on localhost/shri 

8. Compare to production database sanrights/
    python dbdiff2.py --ref-schema intermediate_normalized --comp-schema intermediate_normalized --working-dir ../shri_import
    python dbdiff2.py --ref-schema intermediate_cleaned --comp-schema intermediate_cleaned --working-dir ../shri_import
    python dbdiff2.py --ref-schema prod_aggregated --comp-schema prod_aggregated --working-dir ../shri_import
    python dbdiff2.py --ref-schema prod_final --comp-schema prod_final --working-dir ../shri_import


Deployment plan

1. In production, github-airbyte, build airbyte/source-kobotoolbox:0.2.0
2. In the Airbyte workspace e091cefa-9f35-425b-b351-c0e88228a37f upgrade the source connector
3. Change the Airbyte source connector in production
4. Reset and re-sync the connection
5. switch to dbt-shri branch 