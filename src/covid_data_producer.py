import time
from json import dumps
from kafka import KafkaProducer
import csv

KAFKA_TOPIC_NAME_CONS = 'covid_data'
KAFKA_BOOTSTRAP_SERVER_CONST = 'localhost:9092'

message = None

if __name__ == '__main__':
    print('Kafka Producer Application Started...')

    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER_CONST, value_serializer=lambda x: dumps(x).encode('utf-8'))

    with open('data/covid_19_clean_complete.csv') as wordwise_data:
        #csv_reader = csv.reader(wordwise_data)
        header = next(wordwise_data)
        csv_reader = wordwise_data.readlines()
        for row in csv_reader:
            #covid_str_data = ','.join(row)
            print(type(row))
            print('Message: ',row)
            kafka_producer.send(KAFKA_TOPIC_NAME_CONS,value=str(row.replace('\n','')))
            time.sleep(0.1)


