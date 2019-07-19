import asyncio, json, time, argparse, settings, os, time
from util import logger
from mqtt import MqttClient

from concurrent.futures import ThreadPoolExecutor

class Error(Exception):
    pass

class EcloudError(Error):
    pass

class WorkerPoolError(Error):
    pass

class TaskQueueError(Error):
    pass

class FileTransfer():

    def __init__(self, source, destination):
        self.source = source
        self.destination = destination

    @classmethod
    def upload(cls, upload, datastore):
        cls.from_definition(upload, cls.WORKER_TO_DATASTORE, task)
        d.destination = '%s:%s' % (
            datastore, os.path.join(settings.DATASTORE_DIR, d.destination))

    @classmethod
    def download(cls, download, datastore):
        d = cls.from_definition(dependency, cls.DATASTORE_TO_WORKER, task)
        d.source = '%s:%s' % (
            datastore, os.path.join(settings.DATASTORE_DIR, d.source))
        return d

    @classmethod
    def from_definition(cls, definition, direction, task):
        p = d = definition
        if isinstance(definition, (list, tuple)):
            p, d = definition
        return cls(source=p, destination=d, direction=direction, task=task)

    def command(self, datastore):
        return ['scp', '-oStrictHostKeyChecking=no', self.source, self.destination]

class Task():

    def __init__(self, key, commands, results, dependencies=[], requirement=[]):
        self.key = key
        self.commands = commands if isinstance(commands, Iterable) else [commands]
        self.results = results
        self.dependencies = set(dependencies)
        self.downloads = [FileTransfer.download(r, datastore) for r in requirements]
        self.uploads = [FileTransfer.upload(r, datastore) for r in results]

    def download(self):
        return [u.command for d in self.download]

    @property
    def task(self):
        return [' && '.join(self.commands)]
        
    def upload(self, datastore):
        return [u.command for d in self.download]
        
    def cleanup(self):
        """Commands for removing uploads."""
        return ['rm %s' % u.path for u in self.uploads]

class Worker():

    def __init__(self, id, keep_alive=False):
        self.id = id
        self.one_id = None
        self.task = None
        self.ip = None
        self.keep_alive = keep_alive

    @property
    def dict(self):
        return {
            'worker_id':self.id,
            'ip': self.ip
        }

class WorkerPool(dict):
    """Simply a stack of workers."""

    available = []
    busy = []
    last_id = -1

    def create(self):
        """Create a new worker. Return the worker_id."""
        id = self.last_id + 1
        worker = Worker(id)
        self.last_id = id
        self.busy.append(id)
        return worker

    def pop(self):
        """Pop an available worker off the stack."""
        if len(self.available) > 0:
            self.available.pop(0)

    def get(worker_id):
        """Retrieve a worker."""
        self.get(worker_id)

    def assign(self, worker_id, task_id):
        """Assign a worker to a task."""
        if not worker_id in self.available:
            raise WorkerPoolException('Unavailable worker cannot be assigned a task.')
        i = self.available.index(worker_id)
        worker = self.get(worker_id)
        worker.task = task_id
        self.available.pop(i)
        self.busy.append(i)

    def is_ready(self, worker_id):
        return worker_id in self.available

    def ready(self, worker_id):
        """Push a worker back on the available stack."""
        assert worker_id in self
        try:
            i = self.busy.index(worker_id)
        except IndexError:
            raise WorkerPoolError('Trying to ready worker that already is ready.')
        self.busy.pop(i)
        self.available.append(worker_id)


class TaskQueue(dict):
    """A task queue."""

    pending=set(), # tasks awaiting dependencies
    queued=set(), # available tasks
    in_progress=set(), 
    blocked=set(), # tasks that cannot run because dependencies failed
    failed=set(),
    succeeded=set(),

    def pop(self):
        """Pop a task of the queue. Return None if no tasks are available."""
        try:
            task = self.queued.pop()
        except IndexError:
            return
        self.move_queue(task, self.in_progress)

    def move_task(task_id, destination):
        source.remove(task_id)
        destination.add(task_id)
        task.queue = destination

    def finish(self, task_id, result):
        # Add to failed or succeeded as appropriate
        task = self[task_id]
        if task.queue != self.in_progress:
            raise TaskQueueError('Tried to finish task that isn\'t in progress.')
        if result['success']:
            self.success(task_id, result)
        else:
            self.fail(task_id, result)

    def fail(self, task_id, result={'reason': 'dependencies failed'}):
        self.move_task(task_id, self.failed)
        for task in self.pending + self.queued:
            if task_id in task.dependencies:
                self.fail(task.id)

    def success(self, task_id):
        self.move_task(task_id, self.succeeded)
        for task in self.pending:
            if task.dependencies <= self.succeeded:
                task.move(self.queued)

    def push(self, task_dict, merge_strategy=None):
        """Push a task onto the queue.

        A merge strategy specifies what to do when the task ID
        already exists.

        keep_succeeded: replace only unfinished and finished but failed
        tasks.

        replace: replace all tasks.
        """
        task = Task.from_dict(task_dict)
        self._push(task, merge_strategy)

    def _push(self, task, merge_strategy):
        if task.key in self:
            if merge_strategy == 'keep_succeeded':
                if task.key in self.finished:
                    return
            elif merge_strategy != 'replace':
                logger.warning('Not adding duplicate task')
                return
        self[task.key] = task
        self.pending.add(task)
        task.queue = 'pending'
        self.update()

    def add_as_succeeded(self, task):
        """Add a mock task."""
        task = Task.from_dict(task)
        task.result = 'Mock'
        self._push(task)

def get_pub_key(path='.ssh/id_rsa.pub'):
    """Read public key from path (default='.ssh/id_rsa.pub')."""
    pub_key = os.path.join(os.environ['HOME'], path)
    if os.path.exists(pub_key):
        with open(pub_key) as f:
            return f.read()
    else:
        logger.warning('No public key found at {}. Generate one with ssh-keygen.')

class Server(MqttClient):

    topics = ['workers', 'control']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pub_key = get_pub_key()
        self.one = pyone.OneServer(settings.ONE_API_ENDPOINT, session="%s:%s" % (settings.ONE_USERNAME, settings.ONE_PASSWORD))
        self.address = self.vm_ip(settings.BOSS_TEMPLATE, settings.VIRTUAL_NETWORK_ID)
        self.datastore_address = self.vm_ip(settings.DATASTORE_TEMPLATE, settings.VIRTUAL_NETWORK_ID, instantiate=True)
        logger.info('Datastore and myself found: {} {}'.format(self.datastore_address, self.address))
        self.task_queue = TaskQueue()
        self.worker_pool = WorkerPool()
        # TODO: load worker and task pools from pickle

    def handle(self, mqtt_message, msg_type, payload={}):
        super().handle(mqtt_message, msg_type=msg_type, payload=payload)

    def on_message(self, client, userdata, msg):
        message = json.loads(msg.payload.decode())
        self.event_loop.call_soon_threadsafe(asyncio.gather, self.handle_async(msg, **message))

    async def control_loop(self):
        while True:
            asyncio.sleep(self.control_interval)
            worker = self.worker_pool.pop()
            if worker is None: continue
            task = self.task_queue.pop()
            if task is None: continue
            self.worker_pool.assign(worker, task)
            self.worker_do(
                worker_id,
                self.deal_task,
                task.download,
                task.task,
                task.upload,
                task.cleanup,
            )
            # Pickle worker and task pools

    async def handle_async(self, mqtt_message, *, msg_type, payload={}):
        await self.event_loop.run_in_executor(None, self.handle, mqtt_message, msg_type, payload)

    def start(self):
        self.connect()
        self.loop_start()
        asyncio.gather([self.control_loop()])

    def start_worker(self, worker):
        """Launch the client script on a worker VM."""
        cmd = '{}/ecloud/env/bin/python ecloud/worker.py {} {} > worker_log'.format(
                self.worker_home_dir, str(worker.worker_id), self.address)
        self.execute_and_detach(cmd, worker.ip, screen_session_name='worker_session')

    def worker_do(self, worker_id, *args, **kwargs):
        self.do('workers/{}'.format(worker_id), *args, **kwargs)

    def execute_and_detach(self, command, remote_host, screen_session_name='remote_task'):
        """Execute a command on a remote host by starting it in a screen session and 
        detaching."""
        remote_exec = [
            'ssh', '-oStrictHostKeyChecking=no', 
            remote_host,
            'screen -dm -S {} {}'.format(screen_session_name, command)
        ]
        self.exec(remote_exec)
        
    def deal_task(self, download, task, upload, cleanup):
        """Send MQTT message to a worked to perform commands.
        
        Must be invoked with self.worker_do.
        """
        return {
            'download':download,
            'task':task,
            'upload':upload,
            'cleanup':cleanup,
        }

    def instantiate(self, worker):
        context = self.one.template.info(
                settings.WORKER_TEMPLATE).TEMPLATE['CONTEXT']
        home = self.worker_home_dir
        eclouddir = home + '/ecloud'
        start_script = (
            '\necho \'{}\' >> /home/ubuntu/.ssh/authorized_keys'.format(self.pub_key) + 
            # Start a process that sleeps for a while, then pings the server and dies.
            # When the ping is received, the server starts the worker process with non-root privileges
            '\n{ecdir}/env/bin/python {ecdir}/worker.py {} {} --sleep --hello > {ecdir}/worker_log'.format(
                worker.id,
                # Assuming boss is also mqtt broker
                self.address,
                ecdir=eclouddir,
            )
        )
        context['START_SCRIPT'] += start_script
        one_id = self.one.template.instantiate(
            settings.WORKER_TEMPLATE,
            '',
            False,
            {'TEMPLATE':{'CONTEXT':context}}, False
        )
        worker.one_id = one_id

    def handle_hello(self, worker_id):
        """Handle worker online message."""
        worker = self.worker_pool.get(worker_id)
        if worker is None:
            logger.warning('Not interacting with unknown worker.')
        else:
            worker.ip = self.vm_ip(settings.WORKER_TEMPLATE, settings.VIRTUAL_NETWORK_ID, worker.one_id)
            self.start_worker(worker)

    def handle_ready(self, worker_id, result=None):
        """Handle worker ready message."""
        print(result)
        # Ready message will be received multiple times if the worker is
        # neither terminated nor assigned a new task.
        if not self.worker_pool.is_ready(worker_id):
            worker = self.worker_pool.get(worker_id)
            task_id = self.task_queue.get(worker.task_id)
            self.worker_pool.ready(worker_id)
            self.task_queue.finish(task_id, result)

    def handle_exception(self, worker_id, exception, traceback):
        logger.error('Worker exception :{}'.format(exception))

    def datastore_file_tree(self):
        find = ['find',  settings.DATASTORE_DIR, '-printf \'%P\n\'']
        command = ['ssh', '-oStrictHostKeyChecking=no', self.datastore_address] + find
        result = self.exec(command)
        return result.stdout.decode()

    def handle_push(self, *, merge_strategy, skip_existing=False, tasks):
        task_dict = {}
        for task in tasks:
            task_dict[task['key']] = task

        logger.info('Queueing tasks.')
        existing_results = self.datastore_file_tree()
        for task in tasks:
            results_exist = all(r in existing_results for r in task['results'])
            if skip_existing and results_exist:
                self.task_queue.add_as_succeeded(task)
            else:
                self.task_queue.push(task, merge_strategy=merge_strategy)

    def handle_status(self):
        print('{} workers created.\n'.format(self.worker_pool.amount) +
            '{} queued of which {} available.'.format(len(self.task_queue.queued), len(self.task_queue.available)) +
            '{} tasks in progress.'.format(len(self.worker_pool.busy)) + 
            '{} succeeded and {} failed.'.format(len(self.task_queue.succeeded), len(self.task_queue.failed)))

    def handle_create_worker(self, n=1, keep_alive=None):
        keep_alive = keep_alive is not None
        for i in range(int(n)):
            worker = self.worker_pool.create(keep_alive=keep_alive)
            self.instantiate(worker)
            logger.info(worker_id)

    def vm_by_template(self, template):
        vmpool = self.one.vmpool.info(-4, -1, -1, -1)
        return [vm for vm in vmpool.VM if vm.TEMPLATE['TEMPLATE_ID'] == str(template)]

    def vm_ip(self, template_id, virtual_network_id, vm_id=None, instantiate=False):
        # TODO: Clunky function...
        vms = self.vm_by_template(template_id)
        if vm_id is not None:
            vms = [vm for vm in vms if vm.ID == vm_id]
        if len(vms) > 1:
            logger.warning('More than one VM found with template ID: {}'.format(template_id))
        if len(vms) == 0:
            if not instantiate:
                raise EcloudError('No running VM with template ID: {}'.format(template_id))
            else:
                # Instantiate VM
                # Start polling loop with timeout (upon timeout, try creation again?)
                pass
        vm = vms[0]
        nics = vm.TEMPLATE['NIC']
        if isinstance(nics, list):
            for nic in nics:
                if nic['NETWORK_ID'] == str(virtual_network_id):
                    return nic['IP']
        else:
            return nics['IP']
        raise EcloudError('VM has no NIC for the specified virtual network.')

if __name__ == '__main__':
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    args = parser.parse_args()

    server = Server(qos=args.qos)
    server.start()
