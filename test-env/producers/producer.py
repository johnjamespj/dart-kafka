import json
from kafka import KafkaProducer

serialzer = lambda v: json.dumps(v).encode('utf-8')
producer = KafkaProducer(bootstrap_servers=['localhost:19092', 'localhost:29092'], value_serializer=serialzer, linger_ms=4000)

for i in range(1000000):
    if i % 100000 == 0:
        print('adding %i messages' % (i))
    producer.send('test_python', value={ 'key': 'msg_%i' % i, 'value': i })
