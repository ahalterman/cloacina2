import pika
import json
import datetime
from bson import json_util


credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='cloacina_process2', durable=True)
channel.confirm_delivery()

def write_to_queue(doc, queue):
    message = json.dumps(doc, default=json_util.default)
    channel.basic_publish(exchange='',
                      routing_key=queue,
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode = 2,
            ))

# take in a source, generate the list of days
with open('source_name_id.json') as source_file:                                                                                                           
    source_dict = json.load(source_file)  

# function to generate all the dates for one source
def generate_day_list(source_start_end, source_dict = source_dict):
    source = source_start_end.rstrip().split(';') # rstrip to get rid of \n
    print source
    if len(source) != 3:
        logger.warning("Source is not of length 3. Formatting problem? {0}".format(source))
    start = datetime.datetime.strptime(source[1], '%Y-%m-%d')
    end = datetime.datetime.strptime(source[2], '%Y-%m-%d')
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
    date_list = [i.strftime("%Y-%m-%d") for i in date_generated]
    source_list = []
    for date in date_list:
        source_list.append({"source_name" : source[0],
                           "source_id" : source_dict[source[0]],
                           "date" : date})
    #source_list = [(source[0], date) for date in date_list]
    return source_list


# function to read in a file with sources and create for all the sources
# source;startdate;enddate\n etc
with open("source_list.csv") as sl:
    all_sources = []
    for line in sl:
        all_sources.extend(generate_day_list(line))

for f in all_sources:
    write_to_queue(f, "cloacina_process2")
