
from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
from datetime import datetime, timedelta
import airflow.hooks.S3_hook
import airflow.contrib.hooks.snowflake_hook as sfhk
import snowflake.connector
#import mysql.connector

database_name = 'DALE_SANDBOX'
table_name = 'TEST'
sfstage = 'CELLTRAK_TEST_ARFLOW1'
file = 'test.csv'


default_args = {
    'owner': 'dale',
    #'start_date': datetime.now(),
	'start_date': datetime(2019, 9, 1),
    'retry_delay': timedelta(minutes=.5)
}

def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('aws_s3_test_bucket')
    #hook.load_file_obj(mysql_tbl, key, bucket_name, replace=False, encrypt=False):
	hook.load_file(filename, key, bucket_name)

snowflake_username = sfhk.SnowflakeHook('snowflake')._get_conn_params()['user']
snowflake_password = sfhk.SnowflakeHook('snowflake')._get_conn_params()['password']
snowflake_account = sfhk.SnowflakeHook('snowflake')._get_conn_params()['account']
snowflake_warehouse = sfhk.SnowflakeHook('snowflake')._get_conn_params()['warehouse']
snowflake_schema = sfhk.SnowflakeHook('snowflake')._get_conn_params()['schema']


def upload_to_snowflake():
	con = snowflake.connector.connect(user = snowflake_username, password = snowflake_password, account = snowflake_account, warehouse=snowflake_warehouse, database=database_name, schema=snowflake_schema)
	cs = con.cursor()
	 
	copy = (" copy into %s from '@%s/%s'"
		" file_format = (type = csv field_delimiter = ','"
		#" field_optionally_enclosed_by = '\"'"
		" skip_header = 0)"
		#" on_error = 'continue'
		";"
		% (table_name, sfstage, file)
    )
	 
	cs.execute(copy)
	cs.close()



# Using the context manager alllows you not to duplicate the dag parameter in each operator
with DAG('S3_dag_test', default_args=default_args, schedule_interval='@once') as dag:

    start_task = DummyOperator(
            task_id='dummy_start'
    )

    upload_to_S3_task = PythonOperator(
		task_id='upload_to_S3',
		python_callable=upload_file_to_S3_with_hook,
		op_kwargs={
			'filename': '/usr/local/file-to-watch-1.csv',
			'key': 'test.csv',
			'bucket_name': 'celltrak-test-arflow1',
		},
		dag=dag)

    upload_file = PythonOperator(
        task_id='upload_to_snowflake_task',
        python_callable=upload_to_snowflake,
        #on_failure_callback = failure_slack_message,
        dag=dag)




# Use arrows to set dependencies between tasks
start_task >> upload_to_S3_task >> upload_file







