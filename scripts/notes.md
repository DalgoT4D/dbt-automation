appid 35dd35d8989859a0443e8e26a0a3f816
apikey vinod.rajasekaran@icloud.com:b2a4933167480da63257dfde579c81d2ad08ef6c
project space sc-baseline
exclude application, updates, attachments

source sneha2021: 2017-10-01T00:00:00Z
source sneha: 2020-10-01T00:00:00Z

connection 2021: staging_2021 zzz-case
connection sneha: append/dedup | indexed_on | all streams except 
- CO Group Meeting Form
- Volunteer Activity Form
- Volunteer Group Registration
- Volunteer-HC/MAS Group Activity



/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/dbdiff.py \
    --ref-schema intermediate \
    --comp-schema intermediate \
    --working-dir dbdiff.intermediate \
    --config-yaml /Users/fatchat/tech4dev/dalgo/dbt-automation/connections.sneha.yaml \
  > dbdiff.intermediate/diffs.txt

/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/dbdiff.py \
    --ref-schema intermediate \
    --comp-schema intermediate_195 \
    --working-dir dbdiff.intermediate \
    --config-yaml /Users/fatchat/tech4dev/dalgo/dbt-automation/connections.sneha.yaml \
  > dbdiff.intermediate/diffs.txt




/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/dbdiff.py \
    --ref-schema prod \
    --comp-schema prod \
    --working-dir dbdiff.prod \
    --config-yaml /Users/fatchat/tech4dev/dalgo/dbt-automation/connections.sneha.yaml \
  > dbdiff.prod/diffs.txt


/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/diffstagingtables.py \
    --config-yaml /Users/fatchat/tech4dev/dalgo/dbt-automation/connections.sneha.yaml \
    --ref-sources-yaml refsources.yml \
    --comp-sources-yaml compsources.yml

/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/diffdbttables.py \
    --config-yaml /Users/fatchat/tech4dev/dalgo/dbt-automation/connections.sneha.yaml \
    --schema intermediate


/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/statisticaldiff.py \
    --config-yaml /Users/fatchat/tech4dev/dalgo/dbt-automation/connections.sneha.yaml \
    --schema intermediate
    

    
/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/diffdestinationsv1v2.py \
    --config-yaml /Users/fatchat/tech4dev/dalgo/dbt-automation/connections.sneha.yaml \
    --working-dir dbdiff.staging


/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/trimdestv2staging.py \
    --config-yaml /Users/fatchat/tech4dev/dalgo/dbt-automation/connections.sneha.yaml \
    --working-dir dbdiff.staging


/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/lookupbyid.py \
    --config-yaml /Users/fatchat/tech4dev/dalgo/dbt-automation/connections.sneha.yaml \
    --tableroot ANC_PNC_Visit \
    --id 71de01ac-31a0-4af7-85eb-c54bf6d40b84

/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/getuniqueness.py \
    --config-yaml config.195.yaml \
    --profile ref \
    --schema intermediate \
    --column _airbyte_emitted_at

/Users/fatchat/tech4dev/dalgo/dbt-automation/venv/bin/python \
  /Users/fatchat/tech4dev/dalgo/dbt-automation/scripts/getuniqueness.py \
    --config-yaml config.195.yaml \
    --profile comp \
    --schema intermediate_195 \
    --column indexed_on

