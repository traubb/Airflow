# Project 4 Airflow Pipelines
This project shows how to setup Apache Airflow and transfering data from s3 buckets to the Postgres in Redshift Serverless Cluster via DAGs (Directed Acyclic Graphs).

## Prerequisites
- Setup IAM User with the policies: AdministratorAccess, AmazoneRedshiftFullAccess and AmazonS3FullAccess. And enable Console Login for this User (required for using Redshift Query Editor)
- Configure Redshift Serverless with public ingress (Set Inbound Rule) and give it access to S3
- Create S3 bucket and copy data from s3://udacity-dend/log_data & s3://udacity-dend/song_data to it
- Set the connections and variables in the Airflow UI and change them update them afterwards in the "set_connections.sh" 

## How to make it run
Run the following script in order make Airflow run in your Udacity workspace:
```chmod 0700 /opt/airflow/start-services.sh
chmod 0700 /opt/airflow/start.sh

airflow users create --email student@example.com --firstname aStudent --lastname aStudent --password admin --role Admin --username admin

airflow scheduler

Nach opt
/home/workspace/airflow/dags/cd0031-automate-data-pipelines/set_connections.sh ```

## Files
In the following are the needed files listed as well is their functionality breefly described


