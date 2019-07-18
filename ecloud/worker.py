from mqtt import MqttClient
from util import logger
import argparse, settings, time, traceback

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
        self.do(self.ready)
        self.loop_forever()

    def hello(self):
        return {'worker_id':self.worker_id}

    def ready(self):
        return {'worker_id':self.worker_id}

    def task_result(self, succeeded, info):
        return {
            'worker_id':self.worker_id,
            'success':succeeded,
            'info':info
        }

    def perform_commands(self, commands):
        for cmd in commands:
            try:
                r = self.exec([cmd], pwd=self.worker_home_dir, shell=True)
            except:
                self.do(self.task_result, False, (
                    'Worker {} failed to execute task\n'.format(r.returncode) +
                    'Command: {}\n'.format(r.args) +
                    'Exception: {}\n'.format(traceback.format_exc())
                ))
                return
            if r.returncode != 0:
                self.do(self.task_result, False, (
                    'Task failed with return code {}\n'.format(r.returncode) +
                    'Command: {}\n'.format(r.args) +
                    'Stderr: {}\n'.format(r.stderr.decode())
                ))
                return
        self.do(self.task_result, True, 'Task succeeded')

    def handle_task(self, commands):
        print(commands)
        self.perform_commands(commands)
        self.do(self.ready)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('worker_id', type=int)
    parser.add_argument('broker_url')
    parser.add_argument('--hello', action='store_true')
    parser.add_argument('-s', '--sleep', action='store_true')
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    args = parser.parse_args()

    if args.sleep:
        time.sleep(settings.WORKER_WAKEUP_TIME)
    worker = Worker(args.worker_id, args.broker_url, qos=args.qos)
    if args.hello:
        worker.connect()
        worker.do(worker.hello)
    else:
        worker.start()
