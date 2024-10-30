
from datetime import datetime, timedelta
from google.cloud import storage
import time
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def holapython():
    print("hola Mundo")

def write_read():
    bucket_name = 'rivarly_newclassics'
    blob_name = 'gt_data_lake/clientes.csv'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        print(f.read())




with DAG(
    'etl.demo.gcp',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['devopsgt@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='Demo DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['demo'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='Inicio',
        bash_command='date',
    )

    t2 = PythonOperator(
        task_id='LoadClients',  # Unique task ID
        python_callable=write_read,  # Python function to run
        provide_context=True,  # Provides context like execution_date
    )

    t3 = PythonOperator(
        task_id='GetAndLoadCensus',  # Unique task ID
        python_callable=holapython,  # Python function to run
        provide_context=True,  # Provides context like execution_date
    )

    t4 = PythonOperator(
        task_id='GetAndLoadCountryCodes',  # Unique task ID
        python_callable=holapython,  # Python function to run
        provide_context=True,  # Provides context like execution_date
    )




    t1 >> [t2, t3, t4]