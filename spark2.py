try:

    from datetime import timedelta, datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator import BashOperator
    from airflow.providers.apache.spark.hooks.spark_submit import *
    from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
    from pyspark.sql import SparkSession
    from pyspark.sql import *

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    print("HELLO ")
    


def second_function_execute(**context):
    print("Is it me you looking for")
   


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
}


with DAG(
        dag_id="spark2",
        schedule_interval="@once",
        default_args=default_args,
        catchup=False) as f:

    first_f = PythonOperator(
        task_id="first",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name":"Soumil Shah"}
    )
    
    spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_job',
    conn_id='spark_default',
    java_class='com.ibm.cdopoc.DataLoaderDB2COS',
    application='local:///opt/spark/examples/jars/cppmpoc-dl-0.1.jar',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='2',
    name='airflowspark-DataLoaderDB2COS',
    verbose=True,
    driver_memory='1g',
    conf={
        'spark.DB_URL': 'jdbc:db2://dashdb-dal13.services.dal.bluemix.net:50001/BLUDB:sslConnection=true;',
        'spark.DB_USER': Variable.get("CEDP_DB2_WoC_User"),
        'spark.DB_PASSWORD': Variable.get("CEDP_DB2_WoC_Password"),
        'spark.DB_DRIVER': 'com.ibm.db2.jcc.DB2Driver',
        'spark.DB_TABLE': 'MKT_ATBTN.MERGE_STREAM_2000_REST_API',
        'spark.COS_API_KEY': Variable.get("COS_API_KEY"),
        'spark.COS_SERVICE_ID': Variable.get("COS_SERVICE_ID"),
        'spark.COS_ENDPOINT': 's3-api.us-geo.objectstorage.softlayer.net',
        'spark.COS_BUCKET': 'data-ingestion-poc',
        'spark.COS_OUTPUT_FILENAME': 'cedp-dummy-table-cos2',
        'spark.kubernetes.container.image': 'ctipka/spark:spark-docker',
        'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark'
        }
 )
    
     
first_f 