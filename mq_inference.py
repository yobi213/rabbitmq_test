import pika
import config as cfg
import requests
import json

def get_model_pred(sentence):
    model_url = cfg.model_url
    pred = requests.get(model_url+sentence).json()
    return pred

class Consumer:
    def __init__(self):
        self.__url = cfg.url
        self.__port = cfg.port
        self.__vhost = cfg.source_vhost
        self.__cred = pika.PlainCredentials(cfg.cred_id, cfg.cred_pw)
        self.__queue = cfg.source_queue
        return

    def on_message(channel, method_frame, header_frame, body):
        print('Received %s' % body)
        return

    def model_inference(channel, method_frame, header_frame, body):
        print('body:',body)
        message = json.loads(body)
        print('message:',message)
        print('model input :',message['text'])
        pred = get_model_pred(message['text'])
        message['pred'] = pred
        print(message)
        print(type(message))
        message = json.dumps(message)
        publisher = Publisher()
        publisher.main(message)
        return

    def main(self):
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.__url, self.__port, self.__vhost, self.__cred))
        chan = conn.channel()
        chan.basic_consume(
            queue = self.__queue, 
            on_message_callback = Consumer.model_inference,
            auto_ack = False
        )
        print('Consumer is starting...')
        chan.start_consuming()
        return

class Publisher:
    def __init__(self):
        self.__url = cfg.url
        self.__port = cfg.port
        self.__vhost = cfg.dest_vhost
        self.__cred = pika.PlainCredentials(cfg.cred_id,cfg.cred_pw)
        self.__queue = cfg.dest_queue;
        return

    def main(self,message):
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.__url, self.__port, self.__vhost, self.__cred))
        chan = conn.channel()
        chan.basic_publish(
            exchange = '',
            routing_key = self.__queue,
            body = message
        )
        conn.close()
        return



consumer = Consumer()
consumer.main()


