from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from plugins.helpers import SqlQueries

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


s3_bucket = 'udacity-dend-warehouse'
log_json_file = "log_json_path.json"


default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}


class CreateTableOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, redshift_conn_id = "", *args, **kwargs):
        
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        queries =  open('/home/workspace/airflow/create_tables.sql', 'r').read()
        redshift.run(queries)
        self.log.info("Tables created ")
        

dag_name = 'udac_example_dag'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1 
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables_in_redshift = CreateTableOperator(
    task_id = 'create_tables_in_redshift',
    redshift_conn_id = 'redshift',
    dag = dag
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    data_format="JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    data_format="JSON",
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.songplay_table_insert, 
    dag=dag
)  


load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    delete_load = True,
    table_name = "users"
)


load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    delete_load = True,
    table_name = "songs"
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    delete_load = True,
    table_name = "artists"
)


load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    delete_load = True,
    table_name = "time"
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["artists", "songplays", "songs", "time", "users"]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_in_redshift
create_tables_in_redshift >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
