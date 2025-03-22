from redis import Redis


host = 'localhost'
port = '6369'
db = 0


def redis_pusher(topic: str, message: str):
    """
    """
    redis = Redis(host=host, port=port, db=db)
    redis.publish(topic, message)


while True:
    inp_message = input('insert message (topic, message):\n')
    topic, message = inp_message.split(',')
    redis_pusher(topic, message)
