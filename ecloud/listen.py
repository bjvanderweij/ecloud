from mqtt import MqttClient
import argparse

class Listener(MqttClient):

    

    def handle(self, mqtt_message, *, msg_type, payload={}):
        print('Message type "{}" topic "{}".\nContents:\n{}'.format(msg_type, mqtt_message.topic, payload))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker_url', default='boss.rhythm-uva.surf-hosted.nl')
    parser.add_argument('-p', '--broker_port', type=int, default=1883)
    parser.add_argument('-t', '--topics', nargs='+', default=['workers', 'control'], help='topics to listen on')
    args = parser.parse_args()

    listener = Listener(broker_url=args.broker_url, topics=args.topics, broker_port=args.broker_port)
    listener.connect()
    listener.loop_forever()
