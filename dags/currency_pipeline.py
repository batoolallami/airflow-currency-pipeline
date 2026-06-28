from airflow.sdk import dag, task,Asset
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import json
import requests

currency_rates_asset=Asset("currency_rates_daily")
@dag(
    dag_id="currency_pipeline",
    schedule="@daily",
    start_date=datetime(2026,6,1),
    catchup=False,
    tags=["currency","finance"]
)

def currency_pipeline():
    #----- task 1 extract
    
    @task.python(
            retries=3,
            retry_delay=timedelta(minutes=2)
    )
    def extract_rates(**kwargs):
        date=kwargs["ds"]
        print(f"extraction rates for {date}")
        # call free API
        try:
            url=f"https://api.frankfurter.app/{date}"
            response=requests.get(url)
            data=response.json()
            print(f"got rate: {data}")
            return data
        except requests.exceptions.RequestException as e:
            print(f"API call failed: {e}")
            raise
    #-------------task2 transform
    @task.python
    def transform_rates(raw_data,**kwargs):
        date=kwargs["ds"]

        try:

        #read which currencies we want
            with open("/opt/airflow/config/cdc_config.json") as f:
                config=json.load(f)
        except FileNotFoundError:
            print("config file missing, using defaults")
            config= {
                "currency_pipeline": {
                    "target_currencies": ["EUR", "GBP", "JPY"]
                }
            }
        currencies=config["currency_pipeline"]["target_currencies"]
        records=[]
        for currency,rate in raw_data["rates"].items():
            if currency in currencies:
                records.append({
                    "date":date,
                    "base":raw_data["base"],
                    "currency":currency,
                    "rate":rate
                })
        return records
    
    #-------------load to postgree
    @task.python(
            retries=2,
            retry_delay=timedelta(minutes=1),
            outlets=[currency_rates_asset]
    )
    def load_to_postgres(records,**kwargs):
        import psycopg2
        print(f"loading {len(records)} records to postgres")
        conn=None
        try:
            conn = psycopg2.connect(
                host ="postgres",
                database="airflow",
                user="airflow",
                password="airflow"
            )
            cur=conn.cursor()
        #create table if not exist
            cur.execute("""
CREATE TABLE IF NOT EXISTS currency_rates(
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    base VARCHAR(10),
                    currency VARCHAR(10),
                    rate float,
                    created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE (date, base, currency)
                    )
""")
        # insert each record
            inserted=0
            for record in records:
                cur.execute("""
INSERT INTO currency_rates (date, base, currency,rate) values (%s,%s,%s,%s) on conflict (date, base, currency) do update set 
                            rate= excluded.rate, created_at=NOW()
""", (record["date"],record["base"], record["currency"],record["rate"]))
                
                inserted+=1
            conn.commit()
            print(f"upserted {inserted} records no duplicate")
            return inserted
        except Exception as e:
            print(f"database error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
                print("connection closed")    
    @task.python
    def update_cdc_config(rows_loaded,**kwargs):
        date=kwargs["ds"]
        # read current config
        with open("/opt/airflow/config/cdc_config.json") as f:
            config=json.load(f)
        # update with new info
        config["currency_pipeline"]["last_run_date"]=date
        config["currency_pipeline"]["last_loaded_date"]=date

        # save updated config
        with open("/opt/airflow/config/cdc_config.json","w") as f:
            json.dump(config, f, indent=4)
        
        print(f"CDC updated for {date}")
        print(f"Rows loaded: {rows_loaded}")

    
    run_dbt=BashOperator(
        
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/currency_dbt && dbt run"
    )
    test_dbt=BashOperator(
        task_id="test_dbt_models",
        bash_command="cd /opt/airflow/currency_dbt && dbt test"
    )

    raw =extract_rates()
    cleaned =transform_rates(raw)
    loaded=load_to_postgres(cleaned)
    cdc=update_cdc_config(loaded)
    cdc >> run_dbt >> test_dbt
currency_pipeline()