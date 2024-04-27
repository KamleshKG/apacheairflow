from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import requests
import json
SQLALCHEMY_SILENCE_UBER_WARNING=1

def get_aqi_data():
    decode_data_london_aqi_data = requests.get('https://api.erg.ic.ac.uk/AirQuality/Annual/MonitoringObjective/GroupName=All/Json').content.decode('utf-8-sig')
    london_aqi_data = json.loads(decode_data_london_aqi_data)
    engine = create_engine(url="mysql+pymysql://root:example@192.168.29.38:3306/test")
    for data in london_aqi_data['SiteObjectives']['Site']:
        for objective in data['Objective']:
            engine.execute(f'INSERT INTO AQIDATA VALUES("{objective["@Year"]}","{data["@SiteCode"]}","{data["@SiteName"]}","{str(data["@Latitude"])}","{str(data["@Longitude"])}","{str(objective["@SpeciesCode"])}","{objective["@SpeciesDescription"]}","{str(objective["@Value"])}")')
default_args={
        'owner':'nikhil',
        'retries':5,
        'retry_delay':timedelta(minutes=2)}
with DAG(
    dag_id='uk_aqi_data',
    description='This is the description',
    start_date=datetime(2024,3,24),
    schedule_interval='@daily',
    default_args=default_args
) as dag:
    task2 = PythonOperator(
        task_id='new',
        python_callable=get_aqi_data
    )
    task2


