from mqtt import MqttClient
from util import logger
import argparse, settings, time

class Worker(MqttClient):

    def __init__(self, worker_id, broker_url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.worker_id = worker_id
        self.broker_url = broker_url
        # Topic to listen on
        self.topics = ['workers/{}'.format(self.worker_id)]
        # Topic to broadcast on
        self.broadcast = 'workers'

    def do(self, *args, **kwargs):
        super().do(self.broadcast, *args, **kwargs)

    # Listen for commands 
    def start(self):
        self.connect()
        self.do(self.hello)

    def hello(self):
        return {'worker_id':self.worker_id}

    def done(self, success):
        return {'success':success}

    def perform_commands(self, commands):
        for cmd in commands:
            p = subprocess.Popen([cmd])
            p.communicate()
            # Capture stderr
            # Capture return status

    def handle_task(self, commands):
        self.perform_commands(commands)
        self.do(task_result, True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('worker_id', type=int)
    parser.add_argument('broker_url')
    parser.add_argument('-s', '--skip_sleep', action='store_true')
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    args = parser.parse_args()

    if not args.skip_sleep:
        time.sleep(settings.WORKER_WAKEUP_TIME)
    worker = Worker(args.worker_id, args.broker_url, qos=args.qos)
    worker.start()
