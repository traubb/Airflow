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
from udacity.common import final_project_sql_statements


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

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
        task_id="create_fact_and_dimension_tables",
        postgres_conn_id="redshift",
        sql='create_tables.sql'
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

airflow_project_dag = airflow_project()