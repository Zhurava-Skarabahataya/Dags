try:

    from datetime import timedelta, datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator import BashOperator
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

_config ={'application':'/home/hadoop/pyspark_script/test_spark_submit.py',
    'master' : 'yarn',
    'deploy-mode' : 'cluster',
    'executor_cores': 1,
    'EXECUTORS_MEM': '2G'
}

dag = DAG(
        dag_id="test_dag",
        schedule_interval="@once",
        default_args=default_args,
        catchup=False) 
        
    spark_op = SparkSubmitOperator(
    task_id="spark", dag=dag,
    **_config)

    first_f = PythonOperator(
        task_id="first",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name":"Soumil Shah"}
    )
    
    second_f = PythonOperator(
        task_id="second",
        python_callable=second_function_execute,
        provide_context=True,
    )
    
    

spark_op >> first_f >> second_f 