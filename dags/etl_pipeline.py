from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2

default_args = {
    'owner': 'rachit',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='etl_api_to_postgres',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def extract_data(**kwargs):
    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    df = pd.DataFrame(response.json())
    df.to_csv('/opt/airflow/data/posts.csv', index=False)

def load_to_postgres(**kwargs):
    df = pd.read_csv('/opt/airflow/data/posts.csv')
    conn = psycopg2.connect(
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS posts;")
    cur.execute("""
        CREATE TABLE posts (
            userId INTEGER,
            id INTEGER PRIMARY KEY,
            title TEXT,
            body TEXT
        );
    """)
    for _, row in df.iterrows():
        cur.execute("INSERT INTO posts (userId, id, title, body) VALUES (%s, %s, %s, %s)",
                    (row['userId'], row['id'], row['title'], row['body']))
    conn.commit()
    cur.close()
    conn.close()

def generate_report(**kwargs):
    conn = psycopg2.connect(
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    df = pd.read_sql("SELECT userId, COUNT(*) AS total_posts FROM posts GROUP BY userId", conn)
    df.to_csv('/opt/airflow/data/report.csv', index=False)
    conn.close()

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

extract_task >> load_task >> report_task
