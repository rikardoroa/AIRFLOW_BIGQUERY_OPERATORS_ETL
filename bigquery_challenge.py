from ast import Index
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from google.cloud.exceptions import NotFound,ClientError
from google.oauth2 import service_account
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
from datetime import timedelta
import os
import json
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv('project_id')
DATASET_NAME = os.getenv('dataset_name')
LOCATION = os.getenv('location')
BUCKET = os.getenv('bucket_name')
PERSON_FILE = os.getenv('person_file')
PERSON_CSV_FILE = os.getenv('person_csv_file')


default_args = {
  'start_date': datetime.today().strftime('%Y-%m-%d')
}

# reading credentials
path = os.path.abspath("airflow/dags/app/keygcp.json")
with open(path, "r") as auth_key:
  key_value = json.load(auth_key)

class BigqueryOperations:
  
  #init variables
  def __init__(self , my_bucket = storage.bucket.Bucket,  client = storage.client.Client, 
  credentials = service_account.Credentials.from_service_account_info(key_value)):
    self.my_bucket = my_bucket
    self.client = client
    self.credentials = credentials

  def create_bucket(self):
    try:
        self.client = storage.Client(credentials=self.credentials)
        # creating the bucket
        bucket = self.client.bucket(BUCKET)
        bucket.storage_class = "COLDLINE"
        self.my_bucket = self.client.create_bucket(bucket, location=LOCATION)
    except NotFound:
        print("Bucket not found!")
    except ClientError:
        print("Bucket already exists!")

  

  def creating_bucket_object(self):
    try:
      #creating storage client
      self.client = storage.Client(credentials=self.credentials)
      #loading object path
      path = os.path.abspath("airflow/dags/app/person_data.json")
      object_bucket = self.client.bucket(BUCKET)
      bucket_list = self.client.list_buckets()
      #creating bucket object
      for bucket_object in bucket_list:
          if BUCKET in bucket_object.name:
              with open(path, "r") as file:
                string_file = json.load(file)
                df= pd.DataFrame(string_file)
                abs_path = os.path.abspath("airflow/dags/app/person_data.csv")
                df2 = df.to_csv(abs_path, index=None)
              with open(abs_path,'r')as csv_file:
                  my_csv_file = csv_file.read()
                  blob = object_bucket.blob(PERSON_CSV_FILE)
                  blob.upload_from_string(my_csv_file)
          else:
              print("error,bucket does not exist!")
    except Exception:
      print("Can't create the object")
  


bigquery_data = BigqueryOperations()

with DAG ('biquery_operations', schedule_interval='@hourly', dagrun_timeout=timedelta(minutes=1), max_active_runs=2, default_args = default_args , catchup=False) as dag:
  bucket_creation = PythonOperator(
          task_id='storage_object_creation',
          python_callable=bigquery_data.create_bucket,
          execution_timeout=timedelta(seconds=20)
      )

  object_creation = PythonOperator(
        task_id='bucket_object_creation',
        python_callable=bigquery_data.creating_bucket_object,
        execution_timeout=timedelta(seconds=20)
    )


  create_dataset = BigQueryCreateEmptyDatasetOperator(
          task_id="create_dataset_bg",
          project_id= PROJECT_ID,
          dataset_id=DATASET_NAME,
          location=LOCATION,
          gcp_conn_id='my_gcp_connection'
      )

  create_table = BigQueryCreateEmptyTableOperator(
          task_id="create_table_bg",
          project_id= PROJECT_ID,
          dataset_id=DATASET_NAME,
          table_id="Person",
          gcp_conn_id='my_gcp_connection',
          google_cloud_storage_conn_id='my_gcp_connection'
      )

  load_json_bigquery = GCSToBigQueryOperator(
          task_id="load_data_in_bigquery",
          bucket=BUCKET,
          source_objects=['person_data.csv'],
          destination_project_dataset_table='ariflowproject.person_data_challenge.Person',
          schema_fields=[{"name": "Name", "type": "STRING", "mode": "NULLABLE"},
                         {"name": "Lastname", "type": "STRING", "mode": "NULLABLE"},
                         {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
                         {"name": "Username", "type": "STRING", "mode": "NULLABLE"},
                         {"name": "Password", "type": "STRING", "mode": "NULLABLE"},
                         {"name": "Email", "type": "STRING", "mode": "NULLABLE"}],
          schema_object='gs://airflowtest_challenge/person_data.csv',
          field_delimiter =",",
          source_format='CSV',
          write_disposition='WRITE_TRUNCATE',
          gcp_conn_id='my_gcp_connection'
      )

  bucket_creation >> object_creation 
  create_dataset >> create_table >> load_json_bigquery
