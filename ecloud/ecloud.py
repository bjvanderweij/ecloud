import os, settings
from util import logger
from collections import Iterable

class Error(Exception):
    pass

class WorkerPoolError(Error):
    pass

class TaskQueueError(Error):
    pass

class FileTransfer():

    def __init__(self, source, destination):
        self.source = source
        self.destination = destination

    @property
    def command(self):
        return ' '.join(['cp', self.source, self.destination])

class NetworkFileTransfer(FileTransfer):

    @property
    def command(self):
        return ' '.join(['scp', '-oStrictHostKeyChecking=no', self.source, self.destination])

class Task():

    def __init__(self, id, commands, results, init=False, dependencies=[], requirements=[]):
        self.id = id
        self.commands = commands if not isinstance(commands, str) else [commands]
        self.results = results
        self.dependencies = set(dependencies)
        self.requirements = requirements
        self.results = results
        self.init = init # flag used to indicate this task is a worker initialization task

    def make_download(self, r):
        source, destination = r if not isinstance(r, str) else (r, r)
        source = os.path.join(settings.LOCAL_DATASTORE_DIR, source)
        return FileTransfer(source, destination)

    def make_upload(self, r):
        source, destination = r if not isinstance(r, str) else (r, r)
        destination = os.path.join(settings.LOCAL_DATASTORE_DIR, destination)
        return FileTransfer(source, destination)

    @property
    def downloads(self):
        return [self.make_download(r) for r in self.requirements]

    @property
    def uploads(self):
        return [self.make_upload(r) for r in self.results]

    @property
    def download(self):
        return [d.command for d in self.downloads]

    @property
    def task(self):
        return self.commands
        
    @property
    def upload(self):
        return [u.command for u in self.uploads]
        
    @property
    def cleanup(self):
        """Commands for removing uploads."""
        return ['rm %s' % u.source for u in self.uploads]

class NetworkTask(Task):

    def __init__(self, *args, datastore, **kwargs):
        super().__init__(*args, **kwargs)
        self.datastore = datastore

    def make_download(self, r):
        source, destination = r if not isinstance(r, str) else (r, r)
        source = os.path.join(settings.DATASTORE_DIR, source)
        source = '{}:{}'.format(self.datastore, source)
        return NetworkFileTransfer(source, destination)

    def make_upload(self, r):
        source, destination = r if not isinstance(r, str) else (r, r)
        destination = os.path.join(settings.DATASTORE_DIR, destination)
        destination = '{}:{}'.format(self.datastore, destination)
        return NetworkFileTransfer(source, destination)

class Worker():

    def __init__(self, id, keep_alive=False):
        self.id = id
        self.one_id = None
        self.task_id = None
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
    next_id = 0

    def create(self, worker_id=None, **kwargs):
        """Create a new worker. Return the worker_id."""
        id = worker_id
        while id in self or id is None:
            id = self.next_id
            self.next_id = id + 1
        worker = Worker(id, **kwargs)
        self.busy.append(id)
        self[id] = worker
        self.busy.insert(0, id)
        return worker

    def move(self, worker_id, source, destination):
        source.pop(source.index(worker_id))
        destination.append(worker_id)

    def lock(self, worker_id):
        self.move(worker_id, self.available, self.busy)

    def free(self, worker_id):
        """Push a worker back on the available stack."""
        self.move(worker_id, self.busy, self.available)

    def get_available(self):
        """Pop an available worker off the stack."""
        if len(self.available) > 0:
            return self.available[0]

    def assign(self, worker_id, task_id):
        """Assign a worker to a task."""
        if not worker_id in self.available:
            raise WorkerPoolError('Unavailable worker cannot be assigned a task.')
        worker = self[worker_id]
        worker.task_id = task_id
        self.lock(worker_id)

    def is_free(self, worker_id):
        return worker_id in self.available


class TaskQueue(dict):
    """A task queue."""

    pending=set() # tasks awaiting dependencies
    queued=set() # available tasks
    in_progress=set() 
    blocked=set() # tasks that cannot run because dependencies failed
    failed=set()
    succeeded=set()

    def get_available(self):
        """Pop a task of the queue. Return None if no tasks are available."""
        if len(self.queued) > 0:
            return next(iter(self.queued))

    def move_task(self, destination, task_id=None):
        if task_id is None:
            task_id = self.queued.pop()
        else:
            task = self[task_id]
            task.queue.remove(task_id)
        task = self[task_id]
        destination.add(task_id)
        task.queue = destination
        return task_id

    def begin(self, task_id):
        self.move_task(self.in_progress, task_id=task_id)

    def finish(self, task_id, result):
        # Add to failed or succeeded as appropriate
        task = self[task_id]
        if task.queue != self.in_progress:
            raise TaskQueueError('Tried to finish task that isn\'t in progress.')
        if result['success']:
            self.success(task_id, result)
        else:
            self.fail(task_id, result)

    def fail(self, failed_task_id, result={'stderr': 'dependencies failed'}):
        logger.info('Task {} failed with stderr: {}\n{}'.format(failed_task_id, result['stderr'].strip(), result['command']))
        self[failed_task_id].result = result
        self.move_task(self.failed, task_id=failed_task_id)
        for task_id in self.pending | self.queued:
            task = self[task_id]
            if failed_task_id in task.dependencies:
                self.fail(task_id)

    def success(self, task_id, result):
        logger.info('Task {} succeeded'.format(task_id))
        self[task_id].result = result
        self.move_task(self.succeeded, task_id=task_id)
        self.update_queue()

    def update_queue(self):
        available = [t for t in self.pending if self[t].dependencies <= self.succeeded]
        for task_id in available:
            self.move_task(self.queued, task_id=task_id)

    def push(self, task, merge_strategy=None):
        """Push a task onto the queue.

        A merge strategy specifies what to do when the task ID
        already exists.

        keep_succeeded: replace only unfinished and finished but failed
        tasks.

        replace: replace all tasks.
        """
        if task.id in self:
            if merge_strategy == 'keep_succeeded':
                if task.id in self.finished:
                    return
            elif merge_strategy != 'replace':
                logger.warning('Not adding duplicate task')
                return
        self[task.id] = task
        self.pending.add(task.id)
        task.queue = self.pending
        self.update_queue()

    def add_as_succeeded(self, task):
        """Add a mock task when results exist."""
        task = Task.from_dict(task)
        self.push(task)
        self.finish(task.id, {'success':True, 'reason':'Results exist'})
