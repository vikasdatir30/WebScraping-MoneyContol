#import airflow modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


from datetime import datetime as dt

#import UDF modules
import Extsitedata  as ext
import Transsitedata as trn
#import Loadsitedata as ld

HDFSpath ="/user/moneycontrol/src/stg_indextbl"
HDFScmd = "hdfs dfs -put -f"

default_args={
"start_date" : dt(2020,8,29),
"owner":"airflow",
"email":['vikasd27@outlook.com']
}

with DAG(dag_id="web_moneycontrol_dag", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
	extobj = ext.Extractsite("/home/cloudera/stg")
	fetch_index = PythonOperator(task_id="Extract_Index", python_callable=extobj.extsitedata)
        
	trnobj = trn.Transformsite(extobj.getstgfile(),"/home/cloudera/tgt")
	trans_index  = PythonOperator(task_id="Transform_index", python_callable=trnobj.transdata)
	cmd=HDFScmd+trnobj.gettgtfile().replace('`','')+" "+HDFSpath
	print(cmd)
	load_to_hdfs = BashOperator(task_id="Load_to_hdfs",bash_command=cmd)
	fetch_index>>trans_index>>load_to_hdfs
