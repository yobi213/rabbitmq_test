import pika
import config as cfg

class Consumer:
    def __init__(self):
        self.__url = cfg.url
        self.__port = cfg.port
        self.__vhost = cfg.dest_vhost
        self.__cred = pika.PlainCredentials(cfg.cred_id, cfg.cred_pw)
        self.__queue = cfg.dest_queue;
        return

    def on_message(channel, method_frame, header_frame, body):
        print('Received %s' % body)
        print(type(body))
        return

    def main(self):
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.__url, self.__port, self.__vhost, self.__cred))
        chan = conn.channel()
        chan.basic_consume(
            queue = self.__queue, 
            on_message_callback = Consumer.on_message,
            auto_ack = False
        )
        print('Consumer is starting...')
        chan.start_consuming()
        return

consumer = Consumer()
consumer.main()


