


from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="daily_report",
    description="Ежедневный отчёт",
    schedule="0 6 * * *",
    start_date=datetime(2026, 9, 15),
    catchup=False,
    tags=["generated"],
) as dag:

    def wrapper_extract_orders(**kwargs):
        """Достаём заказы из базы."""
        def extract_orders(**kwargs):
            """Достаём заказы из базы."""
            print("Извлекаем заказы...")
            return 100

    task_extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=wrapper_extract_orders,
    )

    def wrapper_clean_orders(**kwargs):
        """Чистим данные."""
        def clean_orders(**kwargs):
            """Чистим данные."""
            print("Чистим данные...")
            return True

    task_clean_orders = PythonOperator(
        task_id="clean_orders",
        python_callable=wrapper_clean_orders,
    )

    task_sql_task_1 = SQLExecuteQueryOperator(
        task_id="sql_task_1",
        conn_id="transform",
        sql="""DROP TABLE IF EXISTS daily_sales""",
    )

    task_sql_task_2 = SQLExecuteQueryOperator(
        task_id="sql_task_2",
        conn_id="transform",
        sql="""CREATE TABLE daily_sales AS
SELECT DATE(order_date) AS day, SUM(amount) AS total
FROM orders
GROUP BY DATE(order_date)""",
    )

   
    task_extract_orders >> task_clean_orders >> task_sql_task_1 >> task_sql_task_2
