from datetime import datetime, timedelta
from google.cloud import storage
from pymongo.mongo_client import MongoClient

import psycopg2
import pandas as pd
from io import StringIO

import time
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

uri = Variable.get("url_mongo")
bucket_name = 'rivarly_newclassics2'
blob_name = 'gt_data_lake/RAW_DATA/clientes_regional.csv'
blob_destination = 'gt_data_lake/STAGE_DATA/clientes_cleaned.csv'


def holapython():
    print("hola Mundo")

def read():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_data = blob.download_as_text()
    data = StringIO(csv_data)
    df = pd.read_csv(data)
    #print(df)
    filtered_df = df.loc[(df['country'] == 'GT')]
    write_upload(bucket_name, filtered_df, blob_destination)
    storage_client.close()


def write_upload(bucket_name, df,blob_destination):
    connection = BaseHook.get_connection('google_cloud_default')
    storage_client = storage.Client(
        project=connection.extra_dejson.get('extra__google_cloud_platform__project')

    )
    bucket = storage_client.bucket(bucket_name)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    blobdest = bucket.blob(blob_destination)
    blobdest.upload_from_file(csv_buffer, content_type="text/csv")
    storage_client.close()


def load_customers_postgres():
    bucket_name = 'rivarly_newclassics'
    blob_name = 'gt_data_lake/RAW_DATA/clientes.csv'
    #blob_destination = 'gt_data_lake/clientes_raw.csv'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_data = blob.download_as_text()
    data = StringIO(csv_data)
    df = pd.read_csv(data)
    data = df.values.tolist()
    return data

def load_events_postgres():
    bucket_name = 'rivarly_newclassics'
    blob_name = 'gt_data_lake/RAW_DATA/eventos_ficticios.csv'
    #blob_destination = 'gt_data_lake/clientes_raw.csv'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_data = blob.download_as_text()
    data = StringIO(csv_data)
    df = pd.read_csv(data)
    data = df.values.tolist()
    return data
    

def load_purchases_postgres():
    bucket_name = 'rivarly_newclassics'
    blob_name = 'gt_data_lake/RAW_DATA/eventos_ficticios.csv'
    #blob_destination = 'gt_data_lake/clientes_raw.csv'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_data = blob.download_as_text()
    data = StringIO(csv_data)
    df = pd.read_csv(data)
    data = df.values.tolist()
    return data

def pingMongo():
    client = MongoClient(uri)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)


def postgres_to_mongo():
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    client = MongoClient(uri)
    db = client["Xgo"]
    collection = db["compras"]
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('''
               select  id, firstname , lastname , phone , address , c.type, p.type, p.fecha 
                from customers c 
                join events e 
                on c.id = e.idcliente 
                join purchases p 
                on c.id = p.idcliente ''')
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    df = pd.DataFrame(rows)
    df.columns = ['id','firstname','lastname','phone','address','tipo_cliente', 'tipo_evento', 'fecha']
    print(df)
    nested_df = (
        df.groupby(["id", "firstname","lastname","phone","address","tipo_cliente"])
        .apply(lambda x: x[["tipo_evento", "fecha", ]].to_dict("records"))
        .reset_index(name="purchases")
    )

    print(nested_df)
    data_to_insert = nested_df.to_dict(orient="records")
    if df.any().any():
    
        result = collection.insert_many(data_to_insert)
        client.close()
    else:
        print("All values are False or the DataFrame is empty.")




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

    t5 = PythonOperator(
        task_id='clean_customers',  # Unique task ID
        python_callable=read,  # Python function to run
        provide_context=True,  # Provides context like execution_date
    )

    t3 = PythonOperator(
        task_id='clean_events',  # Unique task ID
        python_callable=load_events_postgres,  # Python function to run
        provide_context=True,  # Provides context like execution_date
    )

    t4 = PythonOperator(
        task_id='clean_purchases',  # Unique task ID
        python_callable=load_purchases_postgres,  # Python function to run
        provide_context=True,  # Provides context like execution_date
    )

    
    t2 = GCSCreateBucketOperator(
        task_id='create_gcs_bucket',
        bucket_name='rivarly_newclassics2',
        gcp_conn_id='google_cloud_default',  # La conexión que configuraste en Airflow
        location='US',  # Especifica la región según tus necesidades
    )

    create_table = PostgresOperator(
        task_id= 'create_tables',
        postgres_conn_id='postgres',
        sql='''
         
         DROP TABLE IF EXISTS customers;
         DROP TABLE IF EXISTS events;
         DROP TABLE IF EXISTS purchases;

         CREATE TABLE customers(
         id TEXT,
         firstname TEXT,
         lastname TEXT,
         phone TEXT,
         address TEXT,
         type TEXT
         );

         CREATE TABLE events(
         evento TEXT,
         idcliente TEXT,
         fecha TEXT
         );

         CREATE TABLE purchases(
         idevento TEXT,
         idcliente TEXT,
         type TEXT,
         fecha TEXT
         );
        '''
    )

    l1 = PostgresOperator(
        task_id= 'load_customers',
        postgres_conn_id='postgres',
        sql='''INSERT INTO customers (id, firstname, lastname, phone, address, type) VALUES ( '101','John', 'Doe', '1234567890', '123 Main St', 'Regular') '''
    )

    l2 = PostgresOperator(
        task_id= 'load_events',
        postgres_conn_id='postgres',
        sql='''INSERT INTO events (evento, idcliente, fecha) VALUES ('addcart','101','2024-09-04 19:01:48') '''
    )

    l3 = PostgresOperator(
        task_id= 'load_purchases',
        postgres_conn_id='postgres',
        sql='''INSERT INTO purchases (idevento, idcliente, type, fecha) VALUES ('1','101','pay','2024-09-04 19:01:48'),
               ('2','101','pay','2024-09-04 19:01:48'), ('3','101','pay','2024-10-04 19:01:48'), ('4','101','pay','2024-04-04 19:01:48');
             '''
    )

    ping_mongo = PythonOperator(
        task_id='ping_mongo',  # Unique task ID
        python_callable=pingMongo,  # Python function to run
        provide_context=True,  # Provides context like execution_date
    )

    load_mongo = PythonOperator(
        task_id='load_mongo',  # Unique task ID
        python_callable=postgres_to_mongo,  # Python function to run
        provide_context=True,  # Provides context like execution_date
    )




    t1 >> t2 >> [t5, t3, t4] 
    #>> create_table >> [l1, l2, l3] >> ping_mongo >> load_mongo