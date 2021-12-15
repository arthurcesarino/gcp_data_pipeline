# [START documentation]
# set up connectivity from airflow to gcp using [key] in json format
# create new bucket - processing-zone [GCSCreateBucketOperator]
# sync files from landing-zone to processing-zone [GCSSynchronizeBucketsOperator]
# list objects on the processing zone [GCSListObjectsOperator]
# create google cloud dataproc cluster [DataprocCreateClusterOperator]
# submit pyspark job to google cloud dataproc cluster [DataprocSubmitPySparJobOperator]
# configure sensor to guarantee completeness of the pyspark job [DataprocJobSensor]
# create dataset on BigQuery [BigQueryCreateEmptyDatasetOperator]
# ingest data from Google Cloud Storage (gcs) to BigQuery [GCSToBigQueryOperator]
# verify count of rows (if not null) [BigQueryCheckOperator]
# delete google cloud dataproc cluster [DataprocDeleteClusterOperator]
#delete bucket processing-zone [GCSDeleteBucketOperator]
# [END documentation]


# [START import_module]
from datetime import datetime
from airflow import DAG
from os import getenv
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSSynchronizeBucketsOperator, GCSListObjectsOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# [END import_module]


# [START env_variables]
GCP_PROJECT_ID = getenv('GCP_PROJECT_ID', 'gcp-f1-pipeline')
REGION = getenv('REGION', 'us-east1')
LOCATION = getenv('LOCATION', 'us-east1')
LANDING_BUCKET_ZONE = getenv('LANDING_BUCKET_ZONE', 'dp-landing-zone')
PROCESSING_BUCKET_ZONE = getenv('PROCESSING_BUCKET_ZONE', 'dp-processing-zone')
CURATED_BUCKET_ZONE = getenv('CURATED_BUCKET_ZONE', 'dp-curated-zone')
DATAPROC_CLUSTER_NAME = getenv('DATAPROC_CLUSTER_NAME', 'dp-spark-f1-cluster')
PYSPARK_URI = getenv('PYSPARK_URI', 'gs://dp-code-repository/pyspark_f1_script.py')
BQ_DATASET_NAME = getenv('BQ_DATASET_NAME', 'f1_analysis')
BQ_TABLE_NAME = getenv('BQ_TABLE_NAME', 'f1_drivers_results ')

GCP_CONN_ID = 'gcp'
# [END env_variables]


# [START default_args]
default_args = {
    'owner': 'Arthur Cesarino',
    'depends_on_past': False,
    'email': 'contatocesarino@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}
# [END default_args]


# [START instantiate_dag]
with DAG(
    dag_id='gcp-gcs-dataproc-bigquery',
    tags=['development', 'cloud storage', 'cloud dataproc', 'google bigquery'],
    default_args= default_args,
    start_date=datetime(year=2021, month=11, day=30),
    schedule_interval='@daily',
    catchup= False
) as dag:
# [END instantiate_dag]


#START set_tasks]
    # create gcp bucket to store json files [sync] ~ dp-processing-zone
    # https://registry.astronomer.io/providers/google/modules/gcscreatebucketoperator
    create_gcs_processing_bucket = GCSCreateBucketOperator(
        task_id= 'create_gcs_processing_bucket',
        bucket_name= PROCESSING_BUCKET_ZONE,
        storage_class= 'REGIONAL',
        location= LOCATION,
        labels= {'env': 'dev', 'team': 'airflow'},
        gcp_conn_id= GCP_CONN_ID
    )

    
    # sync files from landing bucket to processing zone
    # https://registry.astronomer.io/providers/google/modules/gcssynchronizebucketsoperator
    gcs_sync_drivers_landing_to_processing_zone = GCSSynchronizeBucketsOperator(
        task_id= 'gcs_sync_drivers_landing_to_processing_zone',
        source_bucket= LANDING_BUCKET_ZONE,
        source_object= 'files/drivers/',
        destination_bucket= PROCESSING_BUCKET_ZONE,
        destination_object= 'files/drivers/',
        allow_overwrite= True,
        gcp_conn_id= GCP_CONN_ID
    )

    # sync files from landing bucket to processing zone
    # https://registry.astronomer.io/providers/google/modules/gcssynchronizebucketsoperator
    gcs_sync_results_landing_to_processing_zone = GCSSynchronizeBucketsOperator (
        task_id= 'gcs_sync_results_landing_to_processing_zone',
        source_bucket= LANDING_BUCKET_ZONE,
        source_object= 'files/results/',
        destination_bucket= PROCESSING_BUCKET_ZONE,
        destination_object= 'files/results/',
        allow_overwrite= True,
        gcp_conn_id= GCP_CONN_ID
    )

    # list files inside of gcs bucket ~ processing zone
    # https://registry.astronomer.io/providers/google/modules/gcslistobjectsoperator
    list_files_processing_zone = GCSListObjectsOperator(
        task_id= 'list_files_processing_zone',
        bucket= PROCESSING_BUCKET_ZONE,
        gcp_conn_id= GCP_CONN_ID
    )

    # create google dataproc cluster ~ [spark engine]
    # https://registry.astronomer.io/providers/google/modules/dataproccreateclusteroperator
    dp_cluster_config = {
        'master_config': {
            'num_instances': 1,
            'machine_type_uri': 'n1-standard-2',
            'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 100},
            },
        'worker_config': {
            'num_instances': 2,
            'machine_type_uri': 'n1-standard-2',
            'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 100},
        }
        
    }

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id= 'create_dataproc_cluster',
        project_id= GCP_PROJECT_ID,
        cluster_name= DATAPROC_CLUSTER_NAME,
        cluster_config= dp_cluster_config,
        region= REGION,
        use_if_exists= True,
        gcp_conn_id= GCP_CONN_ID
    )


    # submit apache spark job ~ [pyspark] file
    # https://registry.astronomer.io/providers/google/modules/dataprocsubmitjoboperator
    job_py_spark_etl_f1_analysis = {
        'reference': {'project_id': GCP_PROJECT_ID},
        'placement': {'cluster_name': DATAPROC_CLUSTER_NAME},
        'pyspark_job': {'main_python_file_uri': PYSPARK_URI}
    }
    
    py_spark_job_submit = DataprocSubmitJobOperator(
        task_id= 'py_spark_job_submit',
        project_id= GCP_PROJECT_ID,
        location= LOCATION,
        job= job_py_spark_etl_f1_analysis,
        asynchronous= True,
        gcp_conn_id= GCP_CONN_ID
    )


    # monitor google cloud dataproc sensor status of job execution
    # https://registry.astronomer.io/providers/google/modules/dataprocjobsensor
    dataproc_job_sensor = DataprocJobSensor(
        task_id= 'dataproc_job_sensor',
        project_id= GCP_PROJECT_ID,
        location= LOCATION,
        dataproc_job_id= "{{task_instance.xcom_pull(task_ids='py_spark_job_submit')}}",
        poke_interval= 30,
        gcp_conn_id= GCP_CONN_ID
    )

    
    # create dataset for google big query engine 
    # https://registry.astronomer.io/providers/google/modules/bigquerycreateemptydatasetoperator
    bq_create_dataset_f1 = BigQueryCreateEmptyDatasetOperator(
        task_id= 'bq_create_dataset_f1',
        dataset_id= BQ_DATASET_NAME,
        gcp_conn_id= GCP_CONN_ID
    )


    # ingest data into big query table
    # https://registry.astronomer.io/providers/google/modules/gcstobigqueryoperator
    ingest_dt_into_bq_table_f1 = GCSToBigQueryOperator(
        task_id= 'ingest_dt_into_bq_table_f1',
        bucket= CURATED_BUCKET_ZONE,
        source_objects=['f1_results/*.parquet'],
        destination_project_dataset_table= f'{GCP_PROJECT_ID}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}',
        source_format= 'parquet',
        write_disposition= 'WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect= True,
        bigquery_conn_id= GCP_CONN_ID
    )


    # delete apache spark cluster ~ dataproc
    # https://registry.astronomer.io/providers/google/modules/dataprocdeleteclusteroperator
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id= 'delete_dataproc_cluster',
        project_id= GCP_PROJECT_ID,
        region= REGION,
        cluster_name= DATAPROC_CLUSTER_NAME,
        gcp_conn_id= GCP_CONN_ID
    )


    # delete processing zone ~ process of data completed by spark engine
    # https://registry.astronomer.io/providers/google/modules/gcsdeletebucketoperator
    delete_bucket_processing_zone = GCSDeleteBucketOperator(
        task_id= 'delete_bucket_processing_zone',
        bucket_name= PROCESSING_BUCKET_ZONE,
        gcp_conn_id= GCP_CONN_ID
    )
# [END set_tasks]


# [START task_sequence]
create_gcs_processing_bucket >> [gcs_sync_drivers_landing_to_processing_zone, gcs_sync_results_landing_to_processing_zone] >> list_files_processing_zone >> create_dataproc_cluster >> py_spark_job_submit >> dataproc_job_sensor >> bq_create_dataset_f1  >> ingest_dt_into_bq_table_f1 >> [delete_dataproc_cluster, delete_bucket_processing_zone]
# [END task_sequence] 