from datetime import date,datetime
import json
import random
from configparser import ConfigParser
import time
import boto3

config = ConfigParser()
conf = json.load(open("src/config.conf"))

session = boto3.Session(profile_name='default')

kinesis_stream_name = conf.get("variables_kinesis").get("kinesis_stream_name")

session = boto3.Session(profile_name='default')


def get_data():
    return {
        'event_date': date.today().isoformat(),
        'event_time': datetime.now().strftime("%H%M%S"),
        'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
        'price': round(random.random() * 100, 2)}


def generate(stream_name):
    print(stream_name)
    while True:
        kinesis_client = session.client('kinesis')
        data = get_data()
        random_range = str(random.randrange(0, 6))
        print(data, f'test-{random_range}')
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=f'test-{random_range}')
        time.sleep(20)


if __name__ == '__main__':
    generate(kinesis_stream_name)
