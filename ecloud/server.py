import asyncio, json, time, argparse, ecloud, settings, os
from pony.orm import db_session
from ecdb import Task, Worker
from util import logger
from mqtt import MqttClient

from concurrent.futures import ThreadPoolExecutor

class Error(Exception):
    pass

class EcloudError(Error):
    pass

def get_pub_key():
    pub_key = os.path.join(os.environ['HOME'], '.ssh/id_rsa.pub')
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
        self.one = ecloud.get_one_server()
        self.address = self.vm_ip(settings.BOSS_TEMPLATE, settings.VIRTUAL_NETWORK_ID)
        self.datastore_address = self.vm_ip(settings.DATASTORE_TEMPLATE, settings.VIRTUAL_NETWORK_ID)
        logger.info('Datastore and myself found: {} {}'.format(self.datastore_address, self.address))
        #self.available = {}
        #self.blocked = {}
        self.workers = 0
        workers = ecloud.get_workers()

    def vm_by_template(self, template):
        vmpool = self.one.vmpool.info(-4, -1, -1, -1)
        return [vm for vm in vmpool.VM if vm.TEMPLATE['TEMPLATE_ID'] == str(template)]

    def vm_ip(self, template_id, virtual_network_id, vm_id=None):
        # TODO: Clunky function...
        vms = self.vm_by_template(template_id)
        if vm_id is not None:
            vms = [vm for vm in vms if vm.ID == vm_id]
        if len(vms) > 1:
            logger.warning('More than one VM found with template ID: {}'.format(template_id))
        if len(vms) == 0:
            raise EcloudError('No running VM with template ID: {}'.format(template_id))
        vm = vms[0]
        nics = vm.TEMPLATE['NIC']
        for nic in nics:
            if nic['NETWORK_ID'] == str(virtual_network_id):
                return nic['IP']
        raise EcloudError('VM has no NIC for the specified virtual network.')

    def update_available_tasks(self):
        available, blocked = ecloud.get_available_tasks()
        for t in available:
            self.available[t.id] = t
        for t in blocked:
            self.blocked[t.id] = t

    def create_worker_id(self):
        self.workers += 1
        return self.workers - 1

    def instantiate_worker(self):
        context = self.one.template.info(settings.WORKER_TEMPLATE).TEMPLATE['CONTEXT']
        eclouddir = '/home/ubuntu/ecloud'
        home = '/home/ubuntu/'
        worker_id = self.create_worker_id()
        start_script = (
            '\necho \'{}\' >> /home/ubuntu/.ssh/authorized_keys'.format(self.pub_key) + 
            #'\necho {worker_id} > {home}/worker_id'.format(worker_id) + 
            #'\necho {broker_url} > {home}/broker_url'.format(self.broker_url)
            '\n{ecdir}/env/bin/python {ecdir}/worker.py {} {} > {ecdir}/worker_log'.format(
                worker_id,
                # Assuming boss is also mqtt broker
                self.address,
                ecdir=eclouddir,
            )
        )
        context['START_SCRIPT'] += start_script
        one_id = self.one.template.instantiate(settings.WORKER_TEMPLATE, '', False, {'TEMPLATE':{'CONTEXT':context}}, False)
        print(worker_id)
        return worker_id, one_id

    @db_session
    def handle_hello(self, worker_id):
        worker = Worker.get_for_update(lambda w: w.worker_id == worker_id)
        if worker is None:
            logger.warning('Not interacting with unknown worker.')
        worker.ip = self.vm_ip(settings.WORKER_TEMPLATE, settings.VIRTUAL_NETWORK_ID, worker.one_id)
        # TODO: Deal task

    @db_session
    def handle_done(self, worker_id):
        worker = ecloud.get_worker(args.worker_id)

        # Make worker available
        # If task is available now, immediately assign it
        # If blocked tasks are available, leave the worker alive
        # If no blocked tasks are available, terminate the worker

        # Only interact with workers we created
        if worker is not None:
            worker.available = True
            self.update_available_tasks()
            if len(self.available) > 0:
                task = next(self.available.values())
                # Replace by mqtt message?
                ecloud.assign_task(worker.worked_id, task.task_id, self.address)
            elif len(self.blocked) == 0:
                ecloud.terminate_worker(one, worker.worker_id)
        else:
            logger.warning('Not interacting with unknown worker.')

    def handel_push(self, *, merge_strategy, skip_existing=False, tasks):
        task_dict = {}
        for task in tasks:
            task_dict[task['key']] = task

        logger.info('Queueing tasks.')
        existing_results = ecloud.get_datastore_file_tree(self.one)
        for task in tasks:
            results_exist = all(r in existing_results for r in task['results'])
            if skip_existing and results_exist:
                ecloud.add_as_succeeded(task)
            else:
                ecloud.queue_task(task, merge_strategy=merge_strategy)

    def handle_status(self):
        workers = ecloud.get_workers()
        tasks = ecloud.get_tasks(Task.QUEUED)
        available = list(ecloud.get_available_tasks())
        in_progress = ecloud.get_tasks(Task.IN_PROGRESS)
        finished = ecloud.get_tasks(Task.FINISHED)
        succeeded = [t for t in finished if t.result.success]
        failed = [t for t in finished if not t.result.success]
        print('%d workers created.' % len(workers))
        print('%d tasks in queue of which %d are currently available.' % (len(tasks), len(available)))
        print('%d tasks in progress.' % len(in_progress))
        print('%d tasks finished. %d succeeded and %d failed.' % (len(finished), len(succeeded), len(failed)))

    def handle_create_worker(self, n=1, keep_alive=None):
        keep_alive = keep_alive is not None
        for i in range(int(n)):
            worker_id, one_id = self.instantiate_worker()
            worker = ecloud.create_worker(worker_id, one_id, keep_alive)
            logger.info(worker_id)

    def handle(self, msg_type, payload):
        logger.info('Handling {} {}'.format(msg_type, payload))
        handler = getattr(self, 'handle_{}'.format(msg_type))
        handler(**payload)

    async def handle_async(self, msg_type, payload={}):
        await self.event_loop.run_in_executor(None, self.handle, msg_type, payload)

    def start(self):
        self.connect()
        self.loop_start()
        self.event_loop = asyncio.get_event_loop()
        try:
            self.event_loop.run_forever()
        finally:
            self.event_loop.close()

    def on_message(self, client, userdata, msg):
        message = json.loads(msg.payload.decode())
        self.event_loop.call_soon_threadsafe(asyncio.gather, self.handle_async(**message))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    args = parser.parse_args()

    server = Server(qos=args.qos)
    server.start()
