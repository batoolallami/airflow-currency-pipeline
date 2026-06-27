from airflow.sdk import dag, task, Asset
from datetime import datetime, timedelta

currency_rates_asset=Asset("currency_rates_daily")
@dag(
    dag_id="currency_report_pipeline",


    schedule=[currency_rates_asset],
    start_date=datetime(2026, 6, 1),
    catchup=False,  
    tags=["currency", "reporting"])

def currency_report_pipeline():
    @task.python
    def generate_summary_report():
        import psycopg2
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"

        )
        cur=conn.cursor()
        cur.execute("select currency,rate from currency_rates where date=(select max(date) from currency_rates) order by currency")
        rows=cur.fetchall()
        print(" Daily Currency Report:")
        for currency,rate in rows:
            print(f" EUR -> {currency}: {rate}")
        print ("End of Report")
        cur.close()
        conn.close()

    generate_summary_report()
currency_report_pipeline()