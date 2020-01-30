from mqtt import MqttClient
from functools import wraps
import argparse, settings, json, time

actions = []

def action(f):
    actions.append(f.__name__)
    return f

class Controller(MqttClient):

    @action
    def push_tasks_batch(self, tasks_path):
        """Push tasks in batches of 10, then sleep for a bit."""
        with open(tasks_path) as f:
            tasks = json.loads(f.read())
        for i in range(len(tasks) // 10):
            print('pushing batch {}/{}'.format(i, len(tasks) // 10))
            self.do('push_tasks', tasks[i*10:(i+1)*10])
            time.sleep(.1)
        return {}

    @action
    def push_tasks_from_file(self, tasks_path):
        with open(tasks_path) as f:
            tasks = json.loads(f.read())
        for t in tasks:
            print('pushing {}'.format(t))
            self.do('push_tasks', [t])
            time.sleep(.1)
        return {}

    def push_tasks(self, tasks, merge_strategy=None):
        return {'tasks':tasks, 'merge_strategy':merge_strategy}

    @action
    def terminate_workers(self):
        pass

    @action
    def terminate_workers_by_template(self):
        pass

    @action
    def status(self):
        pass

    @action
    def retry_failed(self):
        pass

    @action
    def create_worker(self, n=1, keep_alive=None):
        return {'n':n, 'keep_alive':keep_alive}

    @action
    def deal_task(self, worker_id, task):
        return {'worker_id':worker_id, 'task':task}

    @action
    def test(self):
        # Deal a bunch of test tasks
        ...

    @action
    def do(self, action, *args):
        super().do('control', getattr(self, action), *args)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=actions)
    parser.add_argument('args', nargs='*')
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    parser.add_argument('-b', '--broker_url', default=settings.BROKER_URL)
    parser.add_argument('-p', '--broker_port', type=int, default=1883)
    args = parser.parse_args()

    controller = Controller(broker_url=args.broker_url, broker_port=args.broker_port, qos=args.qos)
    controller.connect()
    getattr(controller, args.action)(*args.args)

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
