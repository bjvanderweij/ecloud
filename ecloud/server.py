import asyncio, json, time, argparse, ecloud, settings, os, time
from pony.orm import db_session, commit
from ecdb import Task, Worker
from util import logger
from mqtt import MqttClient

from concurrent.futures import ThreadPoolExecutor

class Error(Exception):
    pass

class EcloudError(Error):
    pass

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
        self.one = ecloud.get_one_server()
        self.address = self.vm_ip(settings.BOSS_TEMPLATE, settings.VIRTUAL_NETWORK_ID)
        self.datastore_address = self.vm_ip(settings.DATASTORE_TEMPLATE, settings.VIRTUAL_NETWORK_ID, instantiate=True)
        logger.info('Datastore and myself found: {} {}'.format(self.datastore_address, self.address))
        #self.available = {}
        #self.blocked = {}
        self.initialize_worker_id()
        workers = ecloud.get_workers()

    @db_session()
    def initialize_worker_id(self):
        workers = Worker.select()
        if len(workers) == 0:
            self.workers = -1
        else:
            max_id_worker = max(Worker.select(), key=lambda w: w.worker_id)
            self.workers = max_id_worker.worker_id 

    async def control_loop(self):
        while True:
            worker = self.pop_worker()
            if worker is not None:


    def create_worker_id(self):
        self.workers += 1
        return self.workers

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

    def update_available_tasks(self):
        available, blocked = ecloud.get_available_tasks()
        for t in available:
            self.available[t.id] = t
        for t in blocked:
            self.blocked[t.id] = t

    def start_worker(self, worker):
        cmd = '{}/ecloud/env/bin/python ecloud/worker.py {} {} > worker_log'.format(
                self.worker_home_dir, str(worker.worker_id), self.address)
        self.execute_and_detach(cmd, worker.ip, screen_session_name='worker_session')

    def worker_do(self, worker_id, *args, **kwargs):
        self.do('workers/{}'.format(worker_id), *args, **kwargs)

    def execute_and_detach(self, command, remote_host, screen_session_name='remote_task'):
        remote_exec = [
            'ssh', '-oStrictHostKeyChecking=no', 
            remote_host,
            'screen -dm -S {} {}'.format(screen_session_name, command)
        ]
        self.exec(remote_exec)

    def handle_deal_task(self, worker_id, task):
        """Just for testing."""
        self.worker_do(worker_id, self.task, [task])
        
    def task(self, commands):
        return {
            'commands':commands,
        }

    def instantiate_worker(self):
        context = self.one.template.info(settings.WORKER_TEMPLATE).TEMPLATE['CONTEXT']
        home = self.worker_home_dir
        eclouddir = home + '/ecloud'
        worker_id = self.create_worker_id()
        start_script = (
            '\necho \'{}\' >> /home/ubuntu/.ssh/authorized_keys'.format(self.pub_key) + 
            # Start a process that sleeps for a while, then pings the server and dies.
            # When the ping is received, the server starts the worker process with non-root privileges
            '\n{ecdir}/env/bin/python {ecdir}/worker.py {} {} --sleep --hello > {ecdir}/worker_log'.format(
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

    @db_session()
    def handle_hello(self, worker_id):
        """Handle worker online message."""
        worker = Worker.get_for_update(lambda w: w.worker_id == worker_id)
        if worker is None:
            logger.warning('Not interacting with unknown worker.')
        else:
            worker.ip = self.vm_ip(settings.WORKER_TEMPLATE, settings.VIRTUAL_NETWORK_ID, worker.one_id)
            self.start_worker(worker)

    @db_session()
    def handle_ready(self, worker_id):
        """Handle worker ready message.
        
        Pop an available task off the queue and assign to worker.
        If no tasks are available, terminate the worker.
        """
        worker = Worker.get_for_update(worker_id=worker_id)
        task = ecloud.get_available_task()
        if task is None and not worker.keep_alive:
            print('Fake termination of worker')
            #ecloud.terminate_worker(self.one, worker.worker_id)
            return

        task.assign(worker)
        commit()
        ds = self.datastore_address
        time.sleep(1)
        self.worker_do(worker_id, self.task, task.initialize(ds) + task.worker_commands() + task.finalize(ds) + task.clean_up())

    @db_session()
    def handle_task_result(self, worker_id, success, info):
        print(info)
        print(worker_id)
        task = Task.get(lambda t: t.worker.worker_id == worker_id and t.status == Task.IN_PROGRESS)
        task.finish(success, info)

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

    @db_session()
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

    # TODO: loop that checks whether to create workers or to assign tasks

    def handle_create_worker(self, n=1, keep_alive=None):
        keep_alive = keep_alive is not None
        for i in range(int(n)):
            worker_id, one_id = self.instantiate_worker()
            worker = ecloud.create_worker(worker_id, one_id, keep_alive)
            logger.info(worker_id)

    async def handle_async(self, mqtt_message, *, msg_type, payload={}):
        await self.event_loop.run_in_executor(None, self.handle, mqtt_message, msg_type, payload)

    def start(self):
        self.connect()
        self.loop_start()
        self.event_loop = asyncio.get_event_loop()
        try:
            self.event_loop.run_forever()
        finally:
            self.event_loop.close()

    def handle(self, mqtt_message, msg_type, payload={}):
        super().handle(mqtt_message, msg_type=msg_type, payload=payload)

    def on_message(self, client, userdata, msg):
        message = json.loads(msg.payload.decode())
        self.event_loop.call_soon_threadsafe(asyncio.gather, self.handle_async(msg, **message))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    args = parser.parse_args()

    server = Server(qos=args.qos)
    server.start()
