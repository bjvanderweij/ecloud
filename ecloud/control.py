from mqtt import MqttClient
from functools import wraps
import argparse, settings, json, time

actions = []

def action(f):

    actions.append(f.__name__)

    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)
    return wrapper

class Controller(MqttClient):

    @action
    def push_tasks_batch(self, tasks_path):
        with open(tasks_path) as f:
            tasks = json.loads(f.read())
        for i in range(len(tasks) // 10):
            print('pushing batch {}/{}'.format(i, len(tasks) // 10))
            controller.do('control', self.push_tasks, tasks[i*10:(i+1)*10])
            time.sleep(.1)
        return {}

    def push_tasks(self, tasks):
        return {'tasks':tasks}

    @action
    def status(self):
        pass

    @action
    def create_worker(self, n=1, keep_alive=None):
        return {'n':n, 'keep_alive':keep_alive}

    @action
    def deal_task(self, worker_id, task):
        return {'worker_id':worker_id, 'task':task}

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

# Actions from admin.py to be implemented:
# show_task(task_id):
# show_context():
# show_task_commands(task_id):
# show_task_dependencies(task_id):
# show_workers():
# print_task(task):
# show_queued():
# show_failed():
# show_succeeded():
# show_in_progress():
# redo_task(task_id):
# retry_failed():
