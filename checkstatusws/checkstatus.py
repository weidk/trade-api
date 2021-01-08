import pandas as pd
import Tools.DataBaseHelper as DB
import time
import pika
import warnings
from time import sleep

warnings.simplefilter(action = "ignore", category = Warning)
Engine = DB.getEngine('10.28.7.43', 'bond', 'bond', 'VirtualExchange')


if __name__ == '__main__':
    credentials = pika.PlainCredentials('bond', 'bond')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('192.168.87.103', 5672, '/', credentials, heartbeat=0))
    channel = connection.channel()
    channel.exchange_declare(exchange='checkpositionstatus', exchange_type='fanout', durable=True)
    while True:
        try:
            Df = pd.read_sql("SELECT [NotAllowAdd] FROM [InvestSystem].[dbo].[positionpsw]", Engine)
            channel.basic_publish(exchange='checkpositionstatus', routing_key='', body=Df.to_json(orient='records'))
            print('success--  '+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
            sleep(2)
        except:
            sleep(2)
            print('error--  ' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
            pass