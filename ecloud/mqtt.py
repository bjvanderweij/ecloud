import paho.mqtt.client as mqtt, json, settings, subprocess, traceback
from util import logger

class MqttClient(mqtt.Client):

    worker_home_dir = '/home/ubuntu'

    def __init__(self, topics=[], broker_url=settings.BROKER_URL, broker_port=1883, qos=0):
        super().__init__()
        self.broker_url = broker_url
        self.broker_port = broker_port
        self.topics = topics
        self.qos = qos
        self.enable_logger(logger)

    def exec(self, cmd, pwd=None, shell=False):
        logger.debug(cmd)
        r = subprocess.run(cmd, capture_output=True, shell=shell)
        stdout = r.stdout.decode()
        stderr = r.stderr.decode()
        logger.debug(stdout)
        logger.warning(stderr.strip())
        return r

    def connect(self):
        logger.info('connecting to {}'.format(self.broker_url))
        super().connect(self.broker_url, self.broker_port)

    def do(self, topic, message_f, *args, **kwargs):
        """Publish a message of type message_f.__name__ to
        "workers" topic. The payload field is the result of
        calling message_f on *args and **kwargs.
        """
        message = {'msg_type':message_f.__name__}
        payload = message_f(*args, **kwargs)
        if payload is not None: 
            message['payload'] = message_f(*args, **kwargs)
        self.publish_json(topic, message)

    def publish_json(self, topic, payload):
        #print('publishing {} to {}'.format(payload, topic))
        self.publish(topic, json.dumps(payload).encode())

    def on_connect(self, client, userdata, flags, rc):
        for topic in self.topics:
            client.subscribe(topic, qos=self.qos)
            logger.info('subscribed to {}'.format(topic))

    def handle(self, mqtt_message, *, msg_type, payload={}):
        logger.debug('Handling {} on topic {}. Contents: {}'.format(msg_type, mqtt_message.topic, payload))
        #print('Handling {} {} on topic {}'.format(msg_type, payload, mqtt_message.topic))
        handler = getattr(self, 'handle_{}'.format(msg_type))
        handler(**payload)

    def on_message(self, client, userdata, msg):
        try:
            message = json.loads(msg.payload.decode())
            self.handle(msg, **message)
        except:
            print(traceback.format_exc())

