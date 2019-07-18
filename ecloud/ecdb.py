from pony.orm import Database, Required, Optional, Set, db_session, PrimaryKey, select
import settings, os

class ECloudError(Exception):
    pass

@db_session()
def connect():

    db = Database()
    db.bind(**settings.DATABASE)
    return db

db = connect()

### Entities

class Context(db.Entity):

    key = PrimaryKey(str)
    value = Required(str)

class TaskResult(db.Entity):

    task = Required('Task')
    success = Required(bool)
    exit_code = Optional(int)
    reason = Optional(str)

class FileTransfer(db.Entity):

    DATASTORE_TO_WORKER = 0
    WORKER_TO_DATASTORE = 1

    path = Required(str)
    destination = Required(str)
    direction = Required(int)
    task = Required('Task')

    @classmethod
    def from_result(cls, result, task):
        cls.from_definition(result, cls.WORKER_TO_DATASTORE, task)

    @classmethod
    def from_requirement(cls, dependency, task):
        cls.from_definition(dependency, cls.DATASTORE_TO_WORKER, task)

    @classmethod
    def from_definition(cls, definition, direction, task):
        p = d = definition
        if isinstance(definition, (list, tuple)):
            p, d = definition
        return cls(path=p, destination=d, direction=direction, task=task)

    @property
    def definition(self):
        return [self.path, self.destination]

    def command(self, datastore):
        scp = 'scp -oStrictHostKeyChecking=no'
        source = self.path
        destination = self.destination
        if self.direction == self.DATASTORE_TO_WORKER:
            source = '%s:%s' % (datastore, os.path.join(settings.DATASTORE_DIR, source))
        else:
            destination = '%s:%s' % (datastore, os.path.join(settings.DATASTORE_DIR, destination))
        return '%s "%s" "%s"' % (scp, source, destination)

class Task(db.Entity):

    QUEUED = 0
    IN_PROGRESS = 1
    FINISHED = 2

    task_id = PrimaryKey(str)
    commands = Set('TaskCommand')
    transfers = Set('FileTransfer')
    dependencies = Set('Task')
    status = Required(int)
    result = Optional('TaskResult', cascade_delete=True)
    worker = Optional('Worker')
    is_necessary_for = Set('Task')

    @classmethod
    def from_dict(cls, task_dict, status=None):

        if status is None: status = cls.QUEUED 
        task = Task(status=status, task_id=task_dict['key'])

        dependencies = [Task.get(task_id=task_id) for task_id in task_dict.get('depends-on', [])]

        if None in dependencies:
            not_found = [d_id for d_id, d in zip(task_dict['depends-on'], dependencies) if d is None]
            raise ECloudError("Some dependencies (%s) of task %s not found in DB." % (', '.join(not_found), task_dict['key']))

        task.dependencies = dependencies

        task_commands = task_dict['cmd'] if isinstance(task_dict['cmd'], (list, tuple)) else [task_dict['cmd']]

        for i, command in enumerate(task_commands):
            if len(command) > 2000:
                print(command)
            TaskCommand(command=command, task=task, sort_order=i)

        for requirement in task_dict.get('requires', []):
            FileTransfer.from_requirement(requirement, task=task)

        for result in task_dict['results']:
            FileTransfer.from_result(result, task=task)



        return task

    def assign(self, worker):

        self.worker = worker
        self.status = self.IN_PROGRESS

    def finish(self, success, info):

        result = TaskResult(success=success, task=self, reason=info)
        self.result = result
        self.status = self.FINISHED

    def initialize(self, datastore):
    
        datastore_to_worker = self.transfers.select(lambda t: t.direction == FileTransfer.DATASTORE_TO_WORKER)
        return [transfer.command(datastore) for transfer in datastore_to_worker]

    def worker_commands(self):

        task_commands = [command.command for command in self.commands.sort_by(TaskCommand.sort_order)]
        return [' && '.join(task_commands)]
        
    def finalize(self, datastore):

        worker_to_datastore = self.transfers.select(lambda t: t.direction == FileTransfer.WORKER_TO_DATASTORE)
        return [transfer.command(datastore) for transfer in worker_to_datastore]
        
    def clean_up(self):

        worker_to_datastore = self.transfers.select(lambda t: t.direction == FileTransfer.WORKER_TO_DATASTORE)
        return ['rm %s' % transfer.path for transfer in worker_to_datastore]

    @property
    def available(self):

        finished = [t for t in self.dependencies.select(lambda task: task.status == self.FINISHED)]
        return len(finished) == len(self.dependencies) and not self.has_failed

    @property
    def has_failed(self):

        failed = [t for t in self.dependencies.select(lambda t: t.status == self.FINISHED) if not t.result.success]
        return len(failed) != 0

    @property
    def dict(self):

        return {
            'key': self.task_id, 
            'results': [t.definition for t in select(transfer for transfer in FileTransfer if transfer.task == self and transfer.direction == FileTransfer.WORKER_TO_DATASTORE)],
            'requires': [t.definition for t in select(transfer for transfer in FileTransfer if transfer.task == self and transfer.direction == FileTransfer.DATASTORE_TO_WORKER)],
            'depends-on': [task.task_id for task in self.dependencies],
            'result': {
                'success':self.result.success,
                'exit_code':self.result.exit_code,
                'reason':self.result.reason,
            } if self.result is not None else None,
            'worker': self.worker.worker_id if self.worker is not None else None,
            'cmd': [command.command for command in self.commands],
        }

class TaskCommand(db.Entity):
    
    task = Required('Task')
    command = Required(str, 2000)
    sort_order = Required(int)

class Worker(db.Entity):

    worker_id = PrimaryKey(int)
    one_id = Required(int)
    ip = Optional(str)
    tasks = Set('Task')
    keep_alive = Required(bool, default=False)
    pop_task_error = Optional(str)

    @property
    def dict(self):
        return {
            'worker_id':self.worker_id,
            'ip': self.ip
        }

db.generate_mapping(create_tables=True)
