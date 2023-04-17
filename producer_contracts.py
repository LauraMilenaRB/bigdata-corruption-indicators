from datetime import date, datetime
import json
import random
from configparser import ConfigParser
import time
import boto3
import pytz

config = ConfigParser()
conf = json.load(open("src/config.conf"))

session = boto3.Session(profile_name='default')

kinesis_stream_name = conf.get("variables_kinesis").get("kinesis_stream_name")

session = boto3.Session(profile_name='default')


def get_data(id_contract):
    return {
        'event_date': date.today().isoformat(),
        'event_time': datetime.now().strftime("%H%M%S"),
        'id_no_contrato': id_contract,
        'id_nit_entidad': random.choice(['890399010', '890399011', '890399000']),
        'id_nit_proveedor': random.choice(['21082091', '21082091']),
        'id_portafolio': random.choice(['21082091', '21082091']),
        'nombre_proveedor': random.choice(['EXOGENA LTDA',
                                           'MUNICIPIOS ASOCIADOS PARA EL DESARROLLO DEL NORTE DE ANTIOQUIA MADENA']
                                          ),
        'nombre_responsable_fiscal': random.choice(['EXOGENA LTDA',
                                                    'MUNICIPIOS ASOCIADOS PARA EL DESARROLLO DEL NORTE DE ANTIOQUIA MADENA',
                                                    'ABONDANO GUZMAN ESPERANZA']
                                                   ),
        'monto_contrato': round(random.random() * 1000000000, 2)
    }


def generate(stream_name):
    print(stream_name)

    count = 1
    while True:
        kinesis_client = session.client('kinesis')
        data = get_data(count)
        random_range = str(random.randrange(0, 6))
        print(data, f'test-{random_range}')
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=f'test-{random_range}')
        time.sleep(20)
        count += 1


if __name__ == '__main__':
    generate(kinesis_stream_name)
