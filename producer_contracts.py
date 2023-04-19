"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

from datetime import date, datetime
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


def get_data(count):
    return {
        'event_date': date.today().isoformat(),
        'event_time': datetime.now().strftime("%H%M%S"),
        'id_no_contrato': f"CO1.REQ.{str(count).zfill(7)}",
        'id_nit_entidad': str(random.choice(['899999034', '800117801', '800007652' , '890981391', '899999098', str(random.randrange(0, 900000000)).zfill(9)])),
        'id_nit_proveedor': str(random.choice(['1053796734', '12997401', '1061985955', '1121826041', '1094271247', '1128283143', str(random.randrange(0, 900000000)).zfill(9),str(random.randrange(0, 9000000000)).zfill(10)])),
        'id_nit_empresa': str(random.choice(['1096224724', '7537330', '72171628', '32849160', '1140817007',str(random.randrange(0, 9000000)).zfill(7), str(random.randrange(0, 900000000)).zfill(9)])),
        'id_portafolio': str(random.choice(['21082091', '21082091', str(random.randrange(0, 90000000)).zfill(8)])),
        'nombre_proveedor': random.choice(['ALD INSTRUMENTS','NEOGLOBAL SAS', 'ANALYTICA SAS', 'UNIPLES S.A', 'EXOGENA LTDA', 'ERIKA CLARO SANABRIA', 'LUIS HERNANDO DIAZ CARDONA',
                                           'MUNICIPIOS ASOCIADOS PARA EL DESARROLLO DEL NORTE DE ANTIOQUIA MADENA', "COLTEXAS JEANS SA", "SOLUCIONES RAPIDAS LTDA", 'PANIVI S.A.']),
        'nombre_responsable_fiscal': random.choice(['EXOGENA LTDA',
                                                    'MUNICIPIOS ASOCIADOS PARA EL DESARROLLO DEL NORTE DE ANTIOQUIA MADENA',
                                                    'ABONDANO GUZMAN ESPERANZA', "LAURA MILENA RAMOS BERMUDEZ"]
                                                   ),
        'monto_contrato': round(random.random() * 100000000, 3)
    }


def generate(stream_name):
    print(stream_name)
    count = 1100069
    while True:
        kinesis_client = session.client('kinesis')
        data = get_data(count)
        random_range = str(random.randrange(0, 6))
        print(data, f'test-{random_range}')
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=f'test-{random_range}')
        count += 1
        time.sleep(15)


if __name__ == '__main__':
    generate(kinesis_stream_name)
