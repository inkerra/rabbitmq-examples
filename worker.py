import pika
import time
import random


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare('exch.dlx', type='direct')
channel.exchange_declare('exch', type='direct')

channel.queue_declare(queue='q')
channel.queue_declare(queue='q2')
channel.queue_declare(queue='q4')

channel.queue_declare(queue='d2', arguments={
    'x-message-ttl': 6000,
    'x-dead-letter-routing-key': 'dl.2',
    'x-dead-letter-exchange': 'exch'})

channel.queue_declare(queue='d4', arguments={
    'x-message-ttl': 18000,
    'x-dead-letter-routing-key': 'dl.4',
    'x-dead-letter-exchange': 'exch'})

channel.queue_bind(queue='d2', exchange='exch.dlx', routing_key='dl.2')
channel.queue_bind(queue='d4', exchange='exch.dlx', routing_key='dl.4')

channel.queue_bind(queue='q2', exchange='exch', routing_key='dl.2')
channel.queue_bind(queue='q4', exchange='exch', routing_key='dl.4')


def callback(ch, method, properties, body):
    print " [x] Received '%s' with key=%s", (body, method.routing_key)
    if random.random() < 0.4:
        ch.basic_ack(delivery_tag = method.delivery_tag)
        print " [x] Done ", (method, body, properties)
    else:
        key = None
        if method.routing_key == 'q':
            key = 'dl.2'
        elif method.routing_key == 'dl.2':
            key = 'dl.4'
        else:
            print " [x] Rejected ", (method, body, properties)

        if key:
            print " [x] Delayed ", "message='%s' key='%s'" % (body, key)
            channel.basic_publish(exchange='exch.dlx', routing_key=key, body=body,)
        ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_consume(callback, queue='q')
channel.basic_consume(callback, queue='q2')
channel.basic_consume(callback, queue='q4')

channel.start_consuming()
