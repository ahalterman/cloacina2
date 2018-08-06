import pika
import time

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='cloacina_process2', durable=True)
channel.confirm_delivery()


doc_count = 0

for method_frame, properties, body in channel.consume('cloacina_process2'):
    print body
    time.sleep(1)
    channel.basic_ack(method_frame.delivery_tag)
    channel.basic_qos(prefetch_count=1)
    doc_count += 1

    # Escape out of the loop after 10 messages
    #if method_frame.delivery_tag == 50:
    if doc_count > 11:
        channel.cancel()
        break

