
from datetime import datetime, timedelta
from google.cloud import storage
from pymongo.mongo_client import MongoClient
import time
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

uri = "mongodb+srv://mcata:Mk21jm00@cluster0.s5vjz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


def holapython():
    print("hola Mundo")

def read():
    bucket_name = 'rivarly_newclassics'
    blob_name = 'gt_data_lake/clientes.csv'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        print(f.read())


def pingMongo():
    client = MongoClient(uri)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)



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
        task_id='Load_Clients',  # Unique task ID
        python_callable=read,  # Python function to run
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

    create_table = PostgresOperator(
        task_id= 'create_table',
        postgres_conn_id='postgres',
        sql='''
         CREATE TABLE IF NOT EXISTS customers(
         firstname TEXT,
         lastname TEXT,
         phone TEXT,
         address TEXT,
         type TEXT
         )
        '''
    )

    pingmongo = PythonOperator(
        task_id='validate_ping_mongo',  # Unique task ID
        python_callable=pingMongo,  # Python function to run
        provide_context=True,  # Provides context like execution_date
    )




    t1 >> [t2, t3, t4] >> create_table >> pingmongo