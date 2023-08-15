#!/bin/bash
#
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get aws_credentials -o json 
# 
#[{"id": "1", 
# "conn_id": "aws_credentials",
# "conn_type": "aws", 
# "description": "", 
# "host": "", 
# "schema": "", 
# "login": "AKIA4QE4NTH3R7EBEANN", 
# "password": "s73eJIJRbnqRtll0/YKxyVYgrDWXfoRpJCDkcG2m", 
# "port": null, 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "aws://AKIAQNUUCCDAX7FVYL5W:boiSov7OvjeqrX%2Bjzp3arG6IcO9ChgYOinIl7AHD@"
#}]
#
# Copy the value after "get_uri":
#
# For example: aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add aws_credentials --conn-uri 'aws://AKIAQNUUCCDAZXNUBWGA:oDF0DHTBd6cqnWntNsfiGdD0uZadnAl1cSRO2FKL@'
#
#
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get redshift -o json
# 
# [{"id": "3", 
# "conn_id": "redshift", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default.859321506295.us-east-1.redshift-serverless.amazonaws.com", 
# "schema": "dev", 
# "login": "awsuser", 
# "password": "R3dsh1ft", 
# "port": "5439", 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]
#
# Copy the value after "get_uri":
#
# For example: redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add redshift --conn-uri 'redshift://awsuser:Wolfgang1401@default.029301543105.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket traubs-pipeline-source
#
# TO-DO: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines