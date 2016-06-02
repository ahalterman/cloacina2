# cloacina2
Download stories from the LexisNexis WSK API with rate limits

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

`worker_unlimited.py` does not have any way of breaking out once the limit is
reached, but it does have functionality for multiple workers on the same queue.
`worker_limited.py` can stop after a set number of messages but can only handle
one worker at once.
