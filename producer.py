import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

while True:
    message = raw_input('Message: ')
    if not message:
        break
    channel.basic_publish(exchange='', routing_key='q', body=message,)
    print " [x] Sent %r" % (message,)

connection.close()
