import asyncio, json, argparse, pickle
from ecloud import TaskQueue, WorkerPool
from util import logger, os
from mqtt import MqttClient

class Server(MqttClient):

    topics = ['workers', 'control']
    cache_path = 'cache/workers_and_queue.pickle'
    init_task = None # tasks to be executed on a worker after it comes online.
    initialized = set()

    def __init__(self, *args, reset=False, **kwargs):
        super().__init__(*args, **kwargs)
        if not reset and os.path.exists(self.cache_path):
            self.load_state()
        else:
            self.task_queue = TaskQueue()
            self.worker_pool = WorkerPool()
        self.control_interval = 1

    def load_state(self):
        with open(self.cache_path, 'rb') as f:
            self.task_queue, self.worker_pool = pickle.load(f)

    def save_state(self):
        with open(self.cache_path, 'wb') as f:
            pickle.dump([self.task_queue, self.worker_pool], f)

    def handle(self, mqtt_message, msg_type, payload={}):
        super().handle(mqtt_message, msg_type=msg_type, payload=payload)

    def on_message(self, client, userdata, msg):
        message = json.loads(msg.payload.decode())
        self.event_loop.call_soon_threadsafe(asyncio.gather, self.handle_async(msg, **message))

    def get_available_worker(self):
        worker_id = self.worker_pool.get_available()
        if (worker_id is not None and 
                not worker_id in self.initialized and
                self.init_task is not None):
            logger.info('Initializing worker {}'.format(worker_id))
            self.worker_pool.assign(worker_id, 'init')
            self.worker_do(worker_id, self.deal_task, self.init_task)
        else:
            return worker_id

    def assign_task(self, worker_id, task_id):
        task = self.task_queue[task_id]
        self.worker_pool.assign(worker_id, task_id)
        self.task_queue.begin(task_id)
        logger.info('Dealing task {} to worker {}'.format(task_id, worker_id))
        self.worker_do(worker_id, self.deal_task, task)

    def heartbeat(self):
        worker_id = self.get_available_worker()
        task_id = self.task_queue.get_available()
        while not (task_id is None or worker_id is None):
            self.assign_task(worker_id, task_id)
            worker_id = self.get_available_worker()
            task_id = self.task_queue.get_available()

    async def control_loop(self):
        while True:
            self.save_state()
            self.heartbeat()
            await asyncio.sleep(self.control_interval)

    async def handle_async(self, mqtt_message, *, msg_type, payload={}):
        # TODO: is this necessary? Maybe just call handle directly, since most are non-blocking anyway
        await self.event_loop.run_in_executor(None, self.handle, mqtt_message, msg_type, payload)

    def start(self):
        self.connect()
        self.loop_start()
        asyncio.run(self.start_loop())

    async def start_loop(self):
        self.event_loop = asyncio.get_event_loop()
        await asyncio.gather(self.control_loop())

    def worker_do(self, worker_id, *args, **kwargs):
        self.do('workers/{}'.format(worker_id), *args, **kwargs)
        
    def deal_task(self, task):
        """Send MQTT message to a worked to perform commands.
        
        Must be invoked with self.worker_do.
        """
        return {
            'download':task.download,
            'task':task.task,
            'upload':task.upload,
            'cleanup':task.cleanup,
        }

    def handle_ready(self, worker_id, result=None):
        """Handle worker ready message.
        
        If the worker isn't already in the worker pool, add 
        it implicitly and print a warning.
        
        If the worker is not initialized, keep the worker in 
        busy state and deal the initialization task.
        
        Else, if the worker isn't already in available state,
        make it available."""
        if not worker_id in self.worker_pool:
            logger.warning('Implicitly creating worker with ID {}'.format(worker_id))
            self.worker_pool.create(worker_id=worker_id)
        # Ready message will be received multiple times if the worker is
        # neither terminated nor assigned a new task.
        worker = self.worker_pool.get(worker_id)
        if not self.worker_pool.is_free(worker_id):
            self.worker_pool.free(worker_id)
            if worker.task_id is not None:
                if worker.task_id == 'init':
                    if result['success']:
                        logger.info('Worker {} initialized successfully'.format(worker_id))
                        self.initialized.add(worker_id)
                    else:
                        print(result)
                        print('Failed to initialize worker. Stderr: {}\nCommand: {}'.format(result['stderr'], result['command']))
                else:
                    self.task_queue.finish(worker.task_id, result)

    def handle_exception(self, worker_id, exception, traceback):
        logger.error('Worker exception :{}'.format(exception))

    def create_task(self, task_dict):
        return Task(**task_dict)

    def find_results(self):
        raise NotImplementedError()

    def handle_push_tasks(self, *, tasks, merge_strategy=None, skip_existing=False):
        logger.info('Queueing tasks.')
        existing_results = self.find_results()
        for task_dict in tasks:
            task = self.create_task(task_dict)
            if task.init:
                if self.init_task is not None:
                    logger.warning('Replacing init task')
                self.init_task = task
            else:
                results_exist = all(r in existing_results for r in task.results)
                if skip_existing and results_exist:
                    self.task_queue.add_as_succeeded(task)
                else:
                    self.task_queue.push(task, merge_strategy=merge_strategy)

    def handle_status(self):
        print('{} workers created.\n'.format(self.worker_pool.amount) +
            '{} queued of which {} available.'.format(len(self.task_queue.queued), len(self.task_queue.available)) +
            '{} tasks in progress.'.format(len(self.worker_pool.busy)) + 
            '{} succeeded and {} failed.'.format(len(self.task_queue.succeeded), len(self.task_queue.failed)))

    def create_worker(self, keep_alive=None):
        worker = self.worker_pool.create(keep_alive=keep_alive)
        logger.info('Created worker with ID {}'.format(worker.id))
        return worker

    def handle_create_worker(self, n=1, keep_alive=None):
        keep_alive = keep_alive is not None
        for i in range(int(n)):
            self.create_worker(keep_alive=keep_alive)

if __name__ == '__main__':
    queue = TaskQueue()
    with open('test-tasks.json') as f:
        tasks = json.loads(f.read())
        for task_dict in tasks:
            task = Task(datastore='localhost', **task_dict)
            queue.push(task)
    task_id = queue.pop()
    while task_id != None:
        print('pending ' + str(queue.pending))
        print('queued ' + str(queue.queued))
        print('succeeded ' + str(queue.succeeded))
        print('failed ' + str(queue.failed))
        task = queue[task_id]
        print(task.task)
        task_id = queue.pop()

