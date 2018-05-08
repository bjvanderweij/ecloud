import settings, pyone, subprocess, tempfile, os, time, warnings, traceback 
from pony.orm import db_session, select, commit
from ecloud.ecdb import Task, TaskResult, FileTransfer, Worker, Context, TaskCommand
from xmlrpc.client import ProtocolError
# sending mail
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

# DECORATORS

def retry_on_exception(exception, sleep=10, max_attempts=10):

    def decorator(f):

        def wrapper(*args, attempt=0, **kwargs):

            if not attempt > max_attempts:
                try:
                    return f(*args, **kwargs)
                except exception as e:
                    warnings.warn('Failed to do %s (attempt=%d/%d). Args: %s, kwargs: %s. Request returned error: %s' % (f.__name__, attempt, max_attempts, args, kwargs, e))
                    time.sleep(sleep)
                    return wrapper(*args, attempt=attempt+1, **kwargs)

        return wrapper

    return decorator


# FUNCTIONS

def get_one_server():

    return pyone.OneServer(settings.ONE_API_ENDPOINT, session="%s:%s" % (settings.ONE_USERNAME, settings.ONE_PASSWORD))

@db_session()
def drop_tasks(status=None):

    if status is None:
        Task.select(lambda t: True).delete()
    else:
        Task.select(lambda t: t.status==status).delete()

@db_session()
def drop_workers():

    all = lambda obj: True
    Worker.select(all).delete()

@db_session()
def drop_context():

    all = lambda obj: True
    Context.select(all).delete()

@db_session()
def queue_task(task, merge_strategy=None):

    if merge_strategy is None:
        return Task.from_dict(task, status=Task.QUEUED)

    t = Task.get(lambda t: t.task_id == task['key'])
    if merge_strategy == 'keep_succeeded':
        # Task exists and did not succeed (yet)
        if t is not None and (not t.result.success if t.result else True):
            t.delete()
            commit()
            return Task.from_dict(task, status=Task.QUEUED)
        elif t is None:
            return Task.from_dict(task, status=Task.QUEUED)
    elif merge_strategy == 'replace':
        if t is None:
            return Task.from_dict(task, status=Task.QUEUED)
        else:
            print('Replacing %s from DB with task from dict.' % t.task_id)
            t.delete()
            commit()
            return Task.from_dict(task, status=Task.QUEUED)

@db_session()
def add_as_succeeded(task, merge_strategy=None):

    t = Task.from_dict(task, status=Task.FINISHED)
    t.result = TaskResult(success=True, task=t, reason='Results exist')
    return t

@db_session()
def get_queued_tasks():

    return select(t for t in Task if t.status == Task.QUEUED)[:]

@db_session()
def get_finished_tasks():

    return select(t for t in Task if t.status == Task.FINISHED)[:]

@db_session()
def get_available_tasks():

    tasks = get_queued_tasks()
    return (t for t in tasks if t.available)

@db_session()
def get_available_task():

   return next(get_available_tasks(), None)

@db_session()
def fail_task(task, reason=''):

    task.status = Task.FINISHED
    task.result = TaskResult(task=task, success=False, reason=reason)

@db_session()
def failed_to_queue():

    failed = select(t for t in Task if t.result.success is False).for_update()
    for task in failed:
        task.result.delete()
        task.status = Task.QUEUED

@db_session()
def create_worker(worker_id):

    return Worker(worker_id=worker_id)

@db_session()
def mark_failed_tasks():

    queue = select(t for t in Task if t.status == Task.QUEUED).for_update()
    failed = (t for t in queue if t.has_failed)
    for task in failed:
        fail_task(task, reason='dependencies failed')

@db_session()
def delete_worker(worker_id):

    Worker.get(worker_id=worker_id).delete()

@db_session()
def get_worker(worker_id):

    return Worker.get(worker_id=worker_id).dict

@db_session()
def get_workers():

    return select(w for w in Worker)[:]

@db_session()
def get_worker(worker_id):

    return Worker.get(worker_id=worker_id)

@retry_on_exception(ProtocolError, sleep=10, max_attempts=settings.MAX_API_ATTEMPTS)
def instantiate_worker(one, boss_address):

    template = settings.WORKER_TEMPLATE
    context = one.template.info(template).TEMPLATE['CONTEXT']
    context['START_SCRIPT'] += '\necho "%s" > /home/ubuntu/boss_address' % boss_address
    worker_id = one.template.instantiate(settings.WORKER_TEMPLATE, '', False, {'TEMPLATE':{'CONTEXT':context}}, False)

    return create_worker(str(worker_id)).worker_id

@retry_on_exception(ProtocolError, sleep=10, max_attempts=settings.MAX_API_ATTEMPTS)
def terminate_worker(one, worker_id):

    one.vm.action('terminate', int(worker_id))
    delete_worker(worker_id)

def send_file(address, name, contents):

    tfile = tempfile.NamedTemporaryFile('w')

    with tfile as f:
        f.write(contents)
        f.flush()
        p = subprocess.Popen(['scp', '-oStrictHostKeyChecking=no', tfile.name, '%s:%s' % (address, name)])
        p.communicate()

@db_session()
def assign_task(worker_id, task_id, boss_address):
    
    task = Task.get_for_update(task_id=task_id)
    worker = Worker.get_for_update(worker_id=worker_id)
    task.assign(worker)
    commit()

    # send task to worker
    send_file(worker.ip, 'initialize', '\n'.join(task.initialize))
    send_file(worker.ip, 'task', '\n'.join(task.worker_commands))
    send_file(worker.ip, 'clean_up', '\n'.join(task.clean_up))
    send_file(worker.ip, 'finalize', '\n'.join(task.finalize))

@db_session
def set_datastore_address(address):
    '''
    Sets or updates the datastore address.
    Return True if set, False if updated.
    '''

    saved = Context.get(lambda c: c.key == 'datastore')
    if saved is None:
        Context(key='datastore', value=address)
    else:
        saved.value = address
    return saved is None

@db_session
def get_datastore_address():

    saved = Context.get(lambda c: c.key == 'datastore')
    if saved is not None: 
        return saved.value

@db_session
def get_datastore_file_tree(one):

    datastore = get_datastore_address()
    boss = get_boss_address(one)
    command = ['find',  settings.DATASTORE_DIR, '-printf \'%P\n\'']
    if boss != datastore: command = ['ssh', '-oStrictHostKeyChecking=no', datastore] + command

    p = subprocess.Popen(command, stdout=subprocess.PIPE)
    file_tree = [path for path in p.stdout.read().decode().split('\n')]

    return file_tree

@db_session
def finish_task(worker_id, init_exit_code, task_exit_code, finalize_exit_code):

    task = Task.get(lambda t: t.worker.worker_id == worker_id and t.status == Task.IN_PROGRESS)
    task.finish(init_exit_code, task_exit_code, finalize_exit_code)

def find_boss(one):

    p = subprocess.Popen(['hostname', '-I'], stdout=subprocess.PIPE)
    addresses = tuple(s.strip() for s in p.stdout.read().decode().strip().split(' '))

    bosses = get_vms_by_template(one, str(settings.BOSS_TEMPLATE))

    vn_id = settings.VIRTUAL_NETWORK_ID
    
    for boss in bosses:
        nics = boss.TEMPLATE['NIC']
        if isinstance(nics, list):
            ip = next((nic['IP'] for nic in nics if nic['NETWORK_ID'] == str(vn_id)), None)
        else:
            ip = nics['IP'] if nics['NETWORK_ID'] == str(vn_id) else None
        if ip in addresses:
            return ip

@db_session()
def get_boss_address(one):

    boss_address = Context.get(key='boss_address')
    if boss_address is None:
        address = find_boss(one)
        boss_address = Context(key='boss_address', value=address)

    return boss_address.value

def get_worker_id_by_ip(one, worker_ip):

    workers = get_vms_by_template(one, str(settings.WORKER_TEMPLATE))
    vn_id = settings.VIRTUAL_NETWORK_ID
    
    for worker in workers:
        # Assume single NIC
        nic = worker.TEMPLATE['NIC']
        if isinstance(nic, list):
            ip = next((nic['IP'] for nic in nics if nic['NETWORK_ID'] == str(vn_id)), None)
        ip = nic['IP']
        if ip == worker_ip:
            return str(worker.ID)

@retry_on_exception(ProtocolError, sleep=10, max_attempts=settings.MAX_API_ATTEMPTS)
def get_vms_by_template(one, template):

    vmpool = one.vmpool.info(-4, -1, -1, -1)
    return [vm for vm in vmpool.VM if vm.TEMPLATE['TEMPLATE_ID'] == template]

@db_session
def keep_worker_alive(worker_id, reason=''):

    worker = Worker.get_for_update(worker_id=worker_id)
    worker.keep_alive = True
    worker.pop_task_error = reason

@db_session
def assign_worker_address(worker_id, address):

    worker = Worker.get_for_update(worker_id=worker_id)
    worker.ip = address
    
@db_session()
def n_available_tasks():
    return len(list(get_available_tasks()))

def fill_worker_pool(one, boss_address):

    workers = get_workers()
    n_workers = len(workers)
    print('Number of workers active: %s' % (len(workers), ))

    n_tasks = n_available_tasks()
    print('Number of tasks available: %s' % n_tasks)

    new_workers = max(min(settings.MAX_WORKERS - n_workers, n_tasks), 0)
    print('Creating %s new workers' % new_workers)

    i = 0
    while new_workers > 0:
        worker_id = instantiate_worker(one, boss_address)
        print('Created %s' % worker_id)
        # Keep checking if more workers are still necessary (worker creation is rather slow)
        i += 1
        n_tasks = n_available_tasks()
        new_workers = max(min(settings.MAX_WORKERS - n_workers - i, n_tasks - i), 0)

 
 # From: https://stackoverflow.com/a/3363254
def send_mail(send_from, send_to, subject, text, files=None,
              server="127.0.0.1"):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)


    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()
