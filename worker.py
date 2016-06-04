import pika
import time
import json
import glob
import csv
import datetime
from multiprocessing import Pool
from pymongo import MongoClient
import logging
import sys
import utilities

# Read config file
ln_user, ln_password, db_collection, whitelist_file, pool_size, log_dir, log_level, auth_db, auth_user, auth_pass, db_host = utilities.parse_config()

# Set up connection to Mongo
if db_host:
    connection = MongoClient(host=db_host)
else:
    connection = MongoClient()

db = connection.lexisnexis
collection = db[db_collection]


# Set up connection to queue.
# The queue contains source-days left to download.
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='cloacina_process2', durable=True)
channel.confirm_delivery()



# Handle the rate limit
global doc_count
doc_count = 0
print doc_count

def download(ch, method, properties, body):
    print(" [x] Received %r" % body)
    global doc_count
    doc_count += 1
    print doc_count
    time.sleep(1)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    if doc_count > 11:
        # use the incorrect nowait arg to kill it. Otherwise it marches on.
        ch.basic_cancel(consumer_tag = '', nowait = True)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(download,
                              queue='cloacina_process2')

channel.start_consuming()
