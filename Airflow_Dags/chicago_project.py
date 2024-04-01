import datetime

from airflow import models
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitSparkSqlJobOperator,
    DataprocSubmitPySparkJobOperator
)
from airflow.utils.dates import days_ago

project_id = Variable.get('project_id')
region = Variable.get('region')
bucket_name = Variable.get('bucket_name')
cluster_name = Variable.get('cluster_name')

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "project_id": project_id,
    "region": region,
    "cluster_name": cluster_name,
    "start_date": days_ago(1)
}

with models.DAG(
    "daily_product_revenue_jobs_dag",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    download_crash = DataprocSubmitPySparkJobOperator(
        task_id='download_crash',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/bronze/download_crash.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    download_poeple = DataprocSubmitPySparkJobOperator(
        task_id='download_poeple',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/bronze/download_poeple.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    download_vehicle = DataprocSubmitPySparkJobOperator(
        task_id='download_vehicle',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/bronze/download_vehicle.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    crash_schema = DataprocSubmitPySparkJobOperator(
        task_id='crash_schema',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/silver/crash_schema.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    people_schema = DataprocSubmitPySparkJobOperator(
        task_id='people_schema',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/silver/people_schema.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    vehicle_schema = DataprocSubmitPySparkJobOperator(
        task_id='vehicle_schema',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/silver/vehicle_schema.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    age_stats = DataprocSubmitPySparkJobOperator(
        task_id='age_stats',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/gold/age_stats.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    crash_hour = DataprocSubmitPySparkJobOperator(
        task_id='crash_hour',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/gold/crash_hour.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    gender_stats = DataprocSubmitPySparkJobOperator(
        task_id='gender_stats',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/gold/gender.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    weather_condition_crash_type = DataprocSubmitPySparkJobOperator(
        task_id='weather_condition_crash_type',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/gold/weather_condition_crash_type.py',
        dataproc_properties={
            'spark.submit.deployMode': 'cluster'
        },
    )
    
    transfer_crash_to_bq = DataprocSubmitPySparkJobOperator(
        task_id='transfer_crash_to_bq',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/transfer/transfer_data_to_bq.py',
        dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar'],
        dataproc_properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://chicago_crash_silver_layer/crash.parquet',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'chicago_crash_warehouse',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name,
            'spark.yarn.appMasterEnv.OUTPUT_NAME': 'crash'
        },
    )
    
    transfer_people_to_bq = DataprocSubmitPySparkJobOperator(
        task_id='transfer_people_to_bq',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/transfer/transfer_data_to_bq.py',
        dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar'],
        dataproc_properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://chicago_crash_silver_layer/people.parquet',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'chicago_crash_warehouse',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name,
            'spark.yarn.appMasterEnv.OUTPUT_NAME': 'people'
        },
    )
    
    transfer_vehicle_to_bq = DataprocSubmitPySparkJobOperator(
        task_id='transfer_vehicle_to_bq',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/transfer/transfer_data_to_bq.py',
        dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar'],
        dataproc_properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://chicago_crash_silver_layer/vehicle.parquet',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'chicago_crash_warehouse',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name,
            'spark.yarn.appMasterEnv.OUTPUT_NAME': 'vehicle'
        },
    )
    
    transfer_age_stats_to_bq = DataprocSubmitPySparkJobOperator(
        task_id='transfer_age_stats_to_bq',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/transfer/transfer_data_to_bq.py',
        dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar'],
        dataproc_properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://chicago_crash_gold_layer/age_stats.parquet',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'chicago_crash_warehouse',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name,
            'spark.yarn.appMasterEnv.OUTPUT_NAME': 'age_stats'
        },
    )
    
    transfer_crash_hour_to_bq = DataprocSubmitPySparkJobOperator(
        task_id='transfer_crash_hour_to_bq',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/transfer/transfer_data_to_bq.py',
        dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar'],
        dataproc_properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://chicago_crash_gold_layer/crash_groupby_hour.parquet',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'chicago_crash_warehouse',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name,
            'spark.yarn.appMasterEnv.OUTPUT_NAME': 'crash_hour_stats'
        },
    )
    
    transfer_gender_to_bq = DataprocSubmitPySparkJobOperator(
        task_id='transfer_gender_to_bq',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/transfer/transfer_data_to_bq.py',
        dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar'],
        dataproc_properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://chicago_crash_gold_layer/gender_counts.parquet',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'chicago_crash_warehouse',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name,
            'spark.yarn.appMasterEnv.OUTPUT_NAME': 'gender_stats'
        },
    )
    
    transfer_weather_condition_to_bq = DataprocSubmitPySparkJobOperator(
        task_id='transfer_weather_condition_to_bq',
        main=f'gs://chicago-data-project-dezoomcamp/jobs/transfer/transfer_data_to_bq.py',
        dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar'],
        dataproc_properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://chicago_crash_gold_layer/weather_condition_crash_type.parquet',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'chicago_crash_warehouse',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name,
            'spark.yarn.appMasterEnv.OUTPUT_NAME': 'weather_condition_and_crash_type'
        },
    )
    
     
download_crash >> crash_schema
download_poeple >> people_schema
download_vehicle >> vehicle_schema

crash_schema >> age_stats
people_schema >> age_stats
vehicle_schema >> age_stats

crash_schema >> crash_hour
people_schema >> crash_hour
vehicle_schema >> crash_hour

crash_schema >> gender_stats
people_schema >> gender_stats
vehicle_schema >> gender_stats

crash_schema >> weather_condition_crash_type
people_schema >> weather_condition_crash_type
vehicle_schema >> weather_condition_crash_type

crash_schema >> transfer_crash_to_bq
people_schema >> transfer_people_to_bq
vehicle_schema >> transfer_vehicle_to_bq
age_stats >> transfer_age_stats_to_bq
crash_hour >> transfer_crash_hour_to_bq
gender_stats >> transfer_gender_to_bq
weather_condition_crash_type >> transfer_weather_condition_to_bq