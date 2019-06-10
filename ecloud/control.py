from mqtt import MqttClient
from functools import wraps
import argparse, settings

actions = []

def action(f):

    actions.append(f.__name__)

    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)
    return wrapper

class Controller(MqttClient):

    @action
    def push_tasks(self, tasks):
        return {'tasks':tasks}

    @action
    def status(self):
        pass

    @action
    def create_worker(self, n=1, keep_alive=None):
        return {'n':n, 'keep_alive':keep_alive}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=actions)
    parser.add_argument('args', nargs='*')
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    parser.add_argument('-b', '--broker_url', default=settings.BROKER_URL)
    args = parser.parse_args()

    controller = Controller(broker_url=args.broker_url, qos=args.qos)
    controller.connect()
    controller.do('control', getattr(controller, args.action), *args.args)

