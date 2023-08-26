# Project 4 Airflow Pipelines
This project shows how to setup Apache Airflow and transfering data from s3 buckets to the Postgres in Redshift Serverless Cluster via DAGs (Directed Acyclic Graphs).

## Prerequisites
- Setup IAM User with the policies: AdministratorAccess, AmazoneRedshiftFullAccess and AmazonS3FullAccess. And enable Console Login for this User (required for using Redshift Query Editor)
- Configure Redshift Serverless with public ingress (Set Inbound Rule) and give it access to S3
- Create S3 bucket and copy data from s3://udacity-dend/log_data & s3://udacity-dend/song_data to it
- Set the connections and variables in the Airflow UI and change them update them afterwards in the "set_connections.sh" 

## How to make it run
Run the following commands in order to make Airflow run in your Udacity workspace:
```
chmod 0700 /opt/airflow/start-services.sh
chmod 0700 /opt/airflow/start.sh

airflow users create --email student@example.com --firstname aStudent --lastname aStudent --password admin --role Admin --username admin

airflow scheduler

/home/workspace/airflow/dags/cd0031-automate-data-pipelines/set_connections.sh
```
Open the Airflow UI

## Files
In the following are the needed files listed as well is their functionality is breefly described:
- project.dag.py: Defines the DAG itself and instantiates the calls the different created Operators from the operators folder
- final_project_sql_statements.py: Includes all necessary SQL statements for creating the following in Redshift: staging_events, staging_songs, users, artists, songplays, time and songs and also for inserting data from staging_events, staging_songs (originated form s3://traubs-airflow-project) to users, artists, songplays, time and songs in Redshift Serverless
- stage_redshift.py: Is a written Operator which copies all JSON files from s3://traubs-airflow-project/log-data/ to the before created staging_events table as well as copies s3://traubs-airflow-project/song-data/ to the before created staging_songs table
- load_fact.py: Operator which inserts the data from the staging_events and staging_songs table to the public.songplays table by using the sql command "songplays_table_insert" from the final_project_sql_statements.py
- load_dimension.py: Operator which inserts the data from the staging_events and staging_songs table to the target dimension tables: public.users, public.songs, public.time, public.artists by using the sql commands "users_table_insert", "artists_table_insert", "songs_table_insert" and "time_table_insert" from the final_project_sql_statements.py
- set_connections.sh: Incorporate your AWS Keys and Secrets here and define your s3_bucket variable


