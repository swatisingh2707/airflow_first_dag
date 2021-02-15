import os
import sys
#sys.path.insert(0,"/c/Users/swati/airflow-docker/assignment_main"))
from datetime import datetime,timedelta
from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import assignment_main_read

global file_path
cwd = os.getcwd()
file_path=cwd+"/dags"
json_input_file='data_20210124.json'
csv_input_file='engagement_20210124.csv'

def call_script():
      assignment_main_read.call_script(file_path,json_input_file,csv_input_file)

'''
      input_file1=cwd+"/dags"
      inputfile2=cwd+"/dags"
      outputfile=cwd+"/dags"
   '''   
      
      
default_args={
      'owner':'airflow',
      'depends_on_past':False,
      'start_date':datetime(2021,2,13),
      'retries':0
      }

dag=DAG(
      dag_id='assignment_main_dag',
      default_args=default_args,
      catchup=False,
      schedule_interval='@once'
      )


t1=DummyOperator(task_id='Start',dag=dag)
#t2=PythonOperator(task_id='get_directory',python_callable=get_directory,dag=dag)
t3=PythonOperator(task_id='assignment_main_task',python_callable=call_script,dag=dag)
t4=DummyOperator(task_id='End',dag=dag)

t1>>t3>>t4
