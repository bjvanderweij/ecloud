from mqtt import MqttClient
from util import logger
import argparse, settings, time, traceback, asyncio

class Worker(MqttClient):

    def __init__(self, worker_id, broker_url, *args, home_dir=None, **kwargs):
        # Topic to listen on
        topics = ['workers/{}'.format(worker_id)]
        super().__init__(*args, topics=topics, **kwargs)
        self.worker_id = worker_id
        self.broker_url = broker_url
        # Topic to broadcast on
        self.broadcast = 'workers'
        # Broadcast isalive every 10 seconds when waiting for tasks
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

    def _make_null_result(self):
        return {
            'success': True,
            'return_code': 0,
            'stderr': '',
            'command':'',
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
        # This just returns the last result, but should somehow summarize them
        r = None
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
        if r is None:
            self.result = self._make_null_result()
        else:
            self.result = self._make_result(r)

    async def wait_loop(self):
        while True:
            if self.commands == None:
                self.do(self.ready)
            await asyncio.sleep(self.interval)

    def handle_deal_task(self, download, task, upload, cleanup):
        self.commands = download + task + upload + cleanup
        try:
            self.perform_commands()
        except Exception as e:
            logger.warning('Worker exception: ' + str(e))
        self.commands = None
        self.do(self.ready)

def start_worker(worker_id, broker_url, broker_port, home_dir=None, qos=1):
    w = Worker(worker_id, broker_url, broker_port=broker_port, qos=qos, home_dir=home_dir)
    w.start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('worker_id', type=int)
    parser.add_argument('broker_url')
    parser.add_argument('--home_dir', help='directory in which to do work')
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
