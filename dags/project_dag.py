from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from airflow.operators.postgres_operator import PostgresOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


s3_bucket = "traubs-airflow-project"
events_s3_key = "log-data"
songs_s3_key = "song-data/A/A/"


default_args = {
    'owner': 'Benny',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False

}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

def airflow_project():

    start_operator = DummyOperator(task_id='Begin_execution')


    create_tables = PostgresOperator(
        task_id='create_fact_and_dimension_tables',
        postgres_conn_id='redshift',  
        sql=[
        SqlQueries.staging_events_table_create,
        SqlQueries.staging_songs_table_create,
        SqlQueries.songplays_table_create,
        SqlQueries.users_table_create,
        SqlQueries.songs_table_create,
        SqlQueries.artists_table_create,
        SqlQueries.time_table_create
        ]
    )

    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="traubs-airflow-project",
        s3_key="log_data/",
        additional_params = {
            "FORMAT": "JSON 's3://traubs-airflow-project/log_json_path.json'"
        }
    )

    # Unfortunately there is an error with the additional_params of the StageToRedshiftOperator here, which could't solve so far. 
    # In generel with adding the log_jon_path.json as a copy format. My ticket is still open:https://knowledge.udacity.com/questions/1003541


    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="traubs-airflow-project",
        s3_key="song_data/",
        additional_params = {
            "JSON": "auto"
        }
    )
    
    
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        target_sql=SqlQueries.songplays_table_insert 
    )


    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        target_sql=SqlQueries.users_table_insert,
        append = False
    )


    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        target_sql=SqlQueries.songs_table_insert,
        append = False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        target_sql=SqlQueries.artists_table_insert,
        append = False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        target_sql=SqlQueries.time_time_insert,
        append = False
    )


    tests = [
    #staging_events
    {'sql': "SELECT COUNT(*) FROM staging_events WHERE session_id IS NULL", 'expected_result': 0},
    #staging_songs
    {'sql': "SELECT COUNT(*) FROM staging_songs WHERE song_id IS NULL", 'expected_result': 0},
    #songplays
    {'sql': "SELECT COUNT(*) songplays WHERE songplay_id IS NULL", 'expected_result': 0},
    #songs
    {'sql': "SELECT COUNT(*) FROM songs WHERE song_id IS NULL", 'expected_result': 0},
    #users
    {'sql': "SELECT COUNT(*) FROM users WHERE user_id IS NULL", 'expected_result': 0},
    #artists
    {'sql': "SELECT COUNT(*) FROM artists WHERE artists_id IS NULL", 'expected_result': 0},
    #time
    {'sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 'expected_result': 0},
    ]

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tests=tests
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table>> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator 
    
airflow_project_dag = airflow_project()
