
#coding=utf-8
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import airflow.utils


# 定义默认参数
default_args = {
    'owner': 'airflow',  # 拥有者名称
        'start_date': airflow.utils.dates.days_ago(1),#  第一次开始执行的时间，为格林威治时间，为了方便测试，一般设置为当前时间减去执行周期
    'email': ['lshan523@163.com'],  # 接收通知的email列表
    'email_on_failure': True,  # 是否在任务执行失败时接收邮件
    'email_on_retry': True,  # 是否在任务重试时接收邮件
    'retries': 3,  # 失败重试次数
    'retry_delay': timedelta(seconds=5),  # 失败重试间隔
    'provide_context': True,
}

# 定义DAG
dag = DAG(
    dag_id='hello_world_args',  # dag_id
    default_args=default_args,  # 指定默认参数
    schedule_interval="@once",
    # schedule_interval="00, *, *, *, *"  # 执行周期，依次是分，时，天，月，年，此处表示每个整点执行
#     schedule_interval=timedelta(minutes=1)  # 执行周期，表示每分钟执行一次
)


# 定义要执行的Python函数1
def hello_world_args_1(**context):
    current_time = str(datetime.today())
    with open('/tmp/hello_world_args_1.txt', 'a') as f:
        f.write('%s\n' % current_time)
    assert 1 == 1  # 可以在函数中使用assert断言来判断执行是否正常，也可以直接抛出异常
    context['task_instance'].xcom_push(key='sea1', value="seaseseseaaa")
    return "t2"

# 定义要执行的Python函数2
def hello_world_args_2(**context):
    sea = context['task_instance'].xcom_pull(key="sea1",task_ids='hello_world_args_1')#参数id，task id
    current_time = str(datetime.today())
    with open('/tmp/hello_world_args_2.txt', 'a') as f:
        f.write('%s\n' % sea)
        f.write('%s\n' % current_time)

# 定义要执行的task 1
t1 = PythonOperator(
    task_id='hello_world_args_1',  # task_id
    python_callable=hello_world_args_1,  # 指定要执行的函数
    dag=dag,  # 指定归属的dag
    retries=2,  # 重写失败重试次数，如果不写，则默认使用dag类中指定的default_args中的设置
    provide_context=True,
)

# 定义要执行的task 2
t2 = PythonOperator(
    task_id='hello_world_args_2',  # task_id
    python_callable=hello_world_args_2,  # 指定要执行的函数
    dag=dag,  # 指定归属的dag
    provide_context=True,
)


t1>>t2

