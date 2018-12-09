FastSync - Postgres to Snowflake
-----------------------------

**This is very quick and very dirty**

Done:
* Generate Snowflake compatible DDL to create destination table. TODO: Add Primary Key
* Export to CSV with Postgres COPY
* Zip
* Upload to S3
* CREATE OR REPLACE target and temp table in Snowflake
* Load into Snowflake temp table with COPY
* Swap temp to to final destination table in snowflake

TODO:
* Obfuscate columns in Snowflake temp table with UPDATE

`postgres-to-snowflake --postgres-config [POSTGRES_TAP_CONFIG] --snowflake-config [SNOWFLAKE_TARGET_CONFIG] --transform-config [TRANSFORMATIONS_CONFIG] --target-schema [SNOWFLAKE_SCHEMA] --tables [LIST_OF_TABLES] --export-dir [TEMP_PATH_DIR]`


Sample config files

## postgres-config.json

  {
    "host": "localhost",
    "port": 5432,
    "dbname": "my_database",
    "schema": "kinesis",
    "user": "my_user",
    "password": "<PASSWORD>",
    "batch_size": 20000
  }

## snowflake-config.json

  {
      "account": "rt123456.eu-central-1",
      "aws_access_key_id": "<ACCESS_KEY_ID>",
      "aws_secret_access_key": "<SECRET_ACCESS_KEY>",
      "dbname": "analytics_db",
      "password": "<PASSWORD>",
      "s3_bucket": "tw-analyticsdb-etl",
      "s3_key_prefix": "snowflake-imports/",
      "user": "analyticsdb_etl",
      "warehouse": "LOAD_WH"
  }

## transformation.json

  {
    "transformations": [
      {
        "stream": "user_profile",
        "fieldId": "last_name",
        "type": "HASH"
      },
      {
        "stream": "user_profile",
        "fieldId": "registration_number",
        "type": "SET_NULL"
      }
    }
  }