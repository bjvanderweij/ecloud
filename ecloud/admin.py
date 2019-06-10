import argparse, ecloud, settings
from ecdb import Task, TaskCommand, Worker, Context
from ecloud import get_workers
from pony.orm import db_session, select
from functools import wraps

actions = []

def action(f):

    actions.append(f.__name__)

    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)
    return wrapper

@action
def instantiate_worker(n=1, keep_alive=None):

    one = ecloud.get_one_server()
    boss_address = ecloud.get_boss_address(one)
    keep_alive = keep_alive is not None
    for i in range(int(n)):
        print(ecloud.instantiate_worker(one, boss_address, keep_alive=keep_alive))

@action
def set_datastore(address):
    ecloud.set_datastore_address(address)

@db_session()
@action
def show_task(task_id):
    
    task = Task.get(lambda t: t.task_id == task_id)
    for cmd in task.commands.sort_by(TaskCommand.sort_order):
        print(cmd.command)

@db_session()
@action
def show_context():
    kv_pairs = Context.select()
    for kv in kv_pairs:
        print('{}: {}'.format(kv.key, kv.value))


@db_session()
@action
def show_task_commands(task_id):
    
    task = Task.get(lambda t: t.task_id == task_id)
    commands = task.initialize + task.worker_commands + task.finalize + task.clean_up
    print('\n'.join(commands))

@db_session()
@action
def show_task_status(task_id):
    
    task = Task.get(lambda t: t.task_id == task_id)
    statuses = ['QUEUED', 'IN_PROGRESS', 'FINISHED']
    print('Status %s' % (statuses[task.status]))
    if task.status > 0:
        print('IP: %s\tIP: %s' % (task.worker.worker_id, task.worker.ip))
    if task.status > 1:
        print('Success: %s\tExit code: %s\tReason: %s' % (task.result.success, task.result.exit_code, task.result.reason))

@db_session()
@action
def show_task_dependencies(task_id):
    
    task = Task.get(lambda t: t.task_id == task_id)
    deps = task.dependencies
    print(' '.join(d.task_id for d in deps))

@db_session()
@action
def show_workers():
    
    workers = ecloud.get_workers()
    for worker in workers:
        tasks = list(worker.tasks.select(lambda t: t.status == Task.IN_PROGRESS))
        task = 'nothing' if len(tasks) == 0 else tasks[0].task_id
        print('ID: %s\tIP: %s\tDoing: %s\tPersistent: %s' % (worker.worker_id, worker.ip, task, worker.keep_alive))

@action
def print_task(task):
    if task.worker is None and task.status != Task.QUEUED:
        print('ID: %s\tExit code: %s\tReason: %s' % (task.task_id, task.result.exit_code, task.result.reason))
    elif task.status != Task.QUEUED:
        print('ID: %s\tWorker ID: %s\tWorker address: %s\tExit code: %s\tReason: %s' % (task.task_id, task.worker.worker_id, task.worker.ip, task.result.exit_code, task.result.reason))
    for cmd in task.commands.sort_by(TaskCommand.sort_order):
        print('\t' + cmd.command)

@db_session()
@action
def show_queued():
    
    queued = Task.select(lambda t: t.status == Task.QUEUED)

    for task in queued:
        print_task(task)

@db_session()
@action
def show_failed():
    
    finished = Task.select(lambda t: t.status == Task.FINISHED)
    tasks = [t for t in finished if not t.result.success]

    for task in tasks:
        print_task(task)

@db_session()
@action
def show_succeeded():
    
    finished = Task.select(lambda t: t.status == Task.FINISHED)
    tasks = [t for t in finished if t.result.success]

    for task in tasks:
        print_task(task)

@db_session()
@action
def show_in_progress():
    
    tasks = Task.select(lambda t: t.status == Task.IN_PROGRESS)

    for task in tasks:
        print('Task %s is being performed by %s (%s)' % (task.task_id, task.worker.worker_id, task.worker.ip))
        for cmd in task.commands.sort_by(TaskCommand.sort_order):
            print(cmd.command)

@db_session()
@action
def add_worker(worker_id):
    
    Worker(worker_id=worker_id)

@db_session()
@action
def drop_workers():
    
    print('This will only delete workers from the database, you will have to terminate them yourself if you haven\'t done so already!')
    ecloud.drop_workers()

@db_session()
@action
def drop_tasks(status=None):
    
    ecloud.drop_tasks(status=status)

@db_session()
@action
def drop_context():
    
    ecloud.drop_context()

@db_session()
@action
def redo_task(task_id):
    task = Task.get(lambda t: t.task_id == task_id)
    task.result.delete()
    task.status = Task.QUEUED

@db_session()
@action
def retry_failed():

    ecloud.failed_to_queue()
    
if __name__  == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=actions)

    parser.add_argument('action_args', nargs='*')
    args = parser.parse_args()

    locals()[args.action](*args.action_args)

