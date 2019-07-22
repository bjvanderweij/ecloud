from mqtt import MqttClient
from util import logger
import argparse, settings, time, traceback, asyncio

class Worker(MqttClient):

    def __init__(self, worker_id, broker_url, *args, home_dir=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.worker_id = worker_id
        self.broker_url = broker_url
        # Topic to listen on
        self.topics = ['workers/{}'.format(self.worker_id)]
        # Topic to broadcast on
        self.broadcast = 'workers'
        # Ping the boss every 10 seconds when waiting for tasks
        self.interval = 10
        self.result = None
        self.commands = None
        if home_dir is not None:
            self.worker_home_dir = home_dir

    def do(self, *args, **kwargs):
        super().do(self.broadcast, *args, **kwargs)

    # Listen for commands 
    def start(self):
        self.connect()
        self.loop_start()
        asyncio.run(self.start_loop())

    async def start_loop(self):
        await asyncio.gather(self.wait_loop())

    def hello(self):
        return {'worker_id':self.worker_id}

    def ready(self):
        return {
            'worker_id':self.worker_id,
            'result':self.result
        }

    def task_result(self, info):
        return {
            'worker_id':self.worker_id,
            'result':result
        }

    def exception(self, exception, tb):
        return {
            'exception':str(exception),
            'traceback':tb,
        }

    def _make_result(self, result):
        if isinstance(result, Exception):
            return {
                'success': False,
                'return_code': None,
                'stderr': None,
                'command':result.command,
                'exception':str(result),
            }
        return {
            'success': result.returncode == 0,
            'return_code': result.returncode,
            'stderr': result.stderr.decode(),
            'command':result.args,
        }

    def perform_commands(self):
        for cmd in self.commands:
            try:
                r = self.exec(cmd, pwd=self.worker_home_dir, shell=True)
            except Exception as e:
                self.do(self.exception, e, traceback.format_exc())
                r = exception
                r.traceback = traceback.format_exc()
                r.command = cmd
                break
            if r.returncode != 0:
                break
        self.result = self._make_result(r)

    async def wait_loop(self):
        while True:
            if self.commands == None:
                self.do(self.ready)
            await asyncio.sleep(self.interval)

    def handle_deal_task(self, download, task, upload, cleanup):
        self.commands = download + task + upload + cleanup
        self.perform_commands()
        self.commands = None
        self.do(self.ready)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('worker_id', type=int)
    parser.add_argument('broker_url')
    parser.add_argument('--home_dir', help='directory in which to work')
    parser.add_argument('--hello', action='store_true')
    parser.add_argument('-s', '--sleep', action='store_true')
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    args = parser.parse_args()

    if args.sleep:
        time.sleep(settings.WORKER_WAKEUP_TIME)
    worker = Worker(args.worker_id, args.broker_url, qos=args.qos, home_dir=args.home_dir)
    if args.hello:
        worker.connect()
        worker.do(worker.hello)
    else:
        worker.start()
