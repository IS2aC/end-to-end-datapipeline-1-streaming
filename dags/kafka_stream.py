import uuid
from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

# define default args for our airflow dag
default_args = {
    "owner" : 'is2ac',
    'start_date' : datetime(2024, 10, 24, 9, 00)
}

def get_data() -> dict:
    """
    data collection function

    Returns:
        dict: random user result as dictionnary instance
    """

    import requests

    res =  requests.get('https://randomuser.me/api/')
    res = res.json()['results'][0]
    return res

def format_data(res:dict) -> dict :
    """
    data transform function, 
    select important informations and dele useless informations.

    Args:
        res (_dict_): random user response of the api request

    Returns:
        dict: random user with important informations extracted.
    """
    
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data() -> None:
    """
    Streaming flow's function pushing data 
    to kafka broker associated to the topic defined : users_created

    """

    import json
    from kafka import KafkaProducer
    import logging
    res =  get_data()
    res =  format_data(res)

    # feel free to play with parameters 
    time_limit =  600


    producer =  KafkaProducer(bootstrap_servers =  ['broker:29092'], max_block_ms = 5000)
    topic_name =  'users_created'
    curr_time = time.time()
    while True:
        if time.time() > curr_time + time_limit : #10 minutes
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send(topic_name, json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occurred : {e}')
            continue


#dag
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily', 
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

    # only one task which first part of our data pipeline.
    streaming_task