import pika
import time

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='cloacina_process2', durable=True)
channel.confirm_delivery()


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(1)
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                              queue='cloacina_process2')

channel.start_consuming()
