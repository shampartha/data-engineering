from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging
import json
from kafka import KafkaProducer
import time

default_args = {
    'owner': 'partha',
    'start_date': datetime(2024, 11, 28, 10, 00)
}

def get_data() -> dict:

    response = requests.get('https://randomuser.me/api/').json()['results'][0]
    return response

def format_data(response: dict):
    data = {}
    location = response['location']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {str(location['street']['name'])}"
    f"{str(location['city'])} {str(location['state'])} {str(location['country'])}"
    data['postcode'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data


def stream_data():

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)  
    current_time = time.time()

    while True:
        try:
            if time.time() > current_time + 60:
                response = get_data()
                user_data = format_data(response= response)
                producer.send('users_create', json.dumps(user_data).encode('utf-8'))
        except Exception as e:
            logging.error(f'error occurred {e}')    
            continue


with DAG('user_automation',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable= stream_data
    )
