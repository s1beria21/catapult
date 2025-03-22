import logging

from redis import Redis


host = 'localhost'
port = '6369'
db = 0


def redis_consumer(topic: str):
    """
    """
    redis = Redis(host=host, port=port, db=db)
    p = redis.pubsub()
    p.subscribe(topic)
    logging.info(f'start consuming [{topic}]...')
    for msg in p.listen():
        if msg['type'] == 'message':
            return msg['data'].decode()
