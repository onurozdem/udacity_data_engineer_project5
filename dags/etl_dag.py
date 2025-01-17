from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                PostgresOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

template_search_path = '/home/workspace/airflow/'

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          template_searchpath = [template_search_path],
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    target_table="staging_events",
    source_s3_path="s3://udacity-dend/log_data",
    aws_credential_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    aws_region="us-west-2",
    json_option="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    target_table="staging_songs",
    source_s3_path="s3://udacity-dend/song_data",
    aws_credential_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    aws_region="us-west-2",
    json_option="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    target_table="songplays",
    redshift_conn_id="redshift",
    stage_table_select_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    target_table="users",
    redshift_conn_id="redshift",
    insert_mode="fresh_insert",
    stage_table_select_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    target_table="songs",
    redshift_conn_id="redshift",
    insert_mode="fresh_insert",
    stage_table_select_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    target_table="artists",
    redshift_conn_id="redshift",
    insert_mode="fresh_insert",
    stage_table_select_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    target_table='"time"',
    redshift_conn_id="redshift",
    insert_mode="fresh_insert",
    stage_table_select_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    control_tables_and_fields={"users":"userid", "songs":"songid", "songplays":"playid"},
    redshift_conn_id="redshift",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_table >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
