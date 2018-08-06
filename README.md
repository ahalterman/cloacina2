# cloacina2

Download stories from the LexisNexis WSK API (if you must) with rate limits

```
pip install pika

sudo apt-get install -y rabbitmq-server
service rabbitmq-server start
```

`python create_queue.py` will read `source_list.csv` and use it to populate a
RabbitMQ queue.

Check what's in each queue and what's been acknowledged from each:

```
sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

`worker.py` will pull sources from the queue, download them, format them, and
load them into MongoDB. A parameter set in the code limits the total number of
articles the worker will download before shutting off. Use `bash
start_workers.sh` to start multiple workers and run them in the background. For
extra speed, multiple workers can use the same username and password, just make
sure to adjust the worker source limit accordingly.

## Note

The customer service I experienced from LexisNexis was terrible: rude, slow,
and technically uninformed. I'd strongly consider other providers if they're
available.
