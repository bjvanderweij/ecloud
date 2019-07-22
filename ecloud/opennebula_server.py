from server import Server
from util import logger, get_pub_key
from ecloud import NetworkTask, Error
import argparse, pyone, settings

class EcloudError(Error):
    pass

class OpenNebulaServer(Server):

    instantiated = {} # dictionary of instantiated VMs
    address = None
    datastore_address = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pub_key = get_pub_key()
        self.one = pyone.OneServer(settings.ONE_API_ENDPOINT, session="%s:%s" % (settings.ONE_USERNAME, settings.ONE_PASSWORD))

    def create_task(self, task_dict):
        return NetworkTask(datastore=self.datastore_address, **task_dict)

    def execute_and_detach(self, command, remote_host, screen_session_name='remote_task'):
        """Execute a command on a remote host by starting it in a screen session and 
        detaching."""
        remote_exec = [
            'ssh', '-oStrictHostKeyChecking=no', 
            remote_host,
            'screen -dm -S {} {}'.format(screen_session_name, command)
        ]
        print(remote_exec)
        self.exec(remote_exec)

    def start_worker(self, worker):
        """Launch the client script on a worker VM."""
        cmd = '{home_dir}/ecloud/env/bin/python ecloud/worker.py {} {} 1>> {home_dir}/log.info 2>> {home_dir}/log.error'.format(
                str(worker.id), self.address, home_dir=self.worker_home_dir)
        self.execute_and_detach(cmd, worker.ip, screen_session_name='worker_session')

    def instantiate(self, template_id, update={'TEMPLATE':{}}):
        one_id = self.one.template.instantiate(
            template_id, '', False, update, False)
        self.instantiated[template_id] = self.instantiated.get(
            template_id, []) + [one_id]
        return one_id

    def create_worker(self, *args, **kwargs):
        worker = super().create_worker(*args, **kwargs)
        context = self.one.template.info(
            settings.WORKER_TEMPLATE).TEMPLATE['CONTEXT']
        start_script = (
            '\nsudo -u ubuntu echo \'{}\' >> /home/ubuntu/.ssh/authorized_keys'.format(self.pub_key) + 
            #'\nsudo -u ubuntu echo "{ecdir}/env/bin/python {ecdir}/worker.py {} {}" > {home_dir}/test'.format(
            #    worker.id,
            #    # Assuming boss is also mqtt broker
            #    self.address,
            #    home_dir=self.worker_home_dir,
            #    ecdir=self.worker_home_dir + '/ecloud',
            #) + 
            '\nsudo -u ubuntu {ecdir}/env/bin/python {ecdir}/worker.py {} {} --hello --sleep 1>> {home_dir}/boot_log.info 2>> {home_dir}/boot_log.error'.format(
                worker.id,
                # Assuming boss is also mqtt broker
                self.address,
                home_dir=self.worker_home_dir,
                ecdir=self.worker_home_dir + '/ecloud',
            )
        )
        context['START_SCRIPT'] += start_script
        one_id = self.instantiate(
            settings.WORKER_TEMPLATE, update={'TEMPLATE':{'CONTEXT':context}})
        worker.one_id = one_id
        return worker

    def datastore_file_tree(self):
        find = ['find',  settings.DATASTORE_DIR, '-printf', '\'%P\n\'']
        command = ['ssh', '-oStrictHostKeyChecking=no', self.datastore_address] + find
        result = self.exec(command)
        return result.stdout.decode()

    def find_results(self):
        return self.datastore_file_tree().split('\n')

    def heartbeat(self):
        if self.address is None:
            self.address = self.find_or_instantiate(settings.BOSS_TEMPLATE, settings.VIRTUAL_NETWORK_ID)
            if self.address is not None:
                logger.info('Myself found: {}'.format(self.address))
        if self.datastore_address is None:
            ds_address = self.find_or_instantiate(settings.DATASTORE_TEMPLATE, settings.VIRTUAL_NETWORK_ID)
            if ds_address is not None:
                self.datastore_address = '{}@{}'.format('ubuntu', ds_address)
                logger.info('Datastore found: {}'.format(self.datastore_address))
        super().heartbeat()

    def handle_hello(self, worker_id):
        """Handle worker online message."""
        if not worker_id in self.worker_pool:
            logger.warning('Implicitly creating worker')
            self.worker_pool.create(worker_id=worker_id)
        worker = self.worker_pool.get(worker_id)
        worker.ip = self.vm_ip(settings.WORKER_TEMPLATE, settings.VIRTUAL_NETWORK_ID, vm_id=worker.one_id)
        logger.info('Setting ip of worker {} to {}'.format(worker_id, worker.ip))
        self.start_worker(worker)

    def find_or_instantiate(self, template_id, virtual_network_id):
        try:
            return self.vm_ip(template_id, virtual_network_id)
        except EcloudError:
            if not template_id in self.instantiated:
                self.instantiate(template_id)

    def vm_by_template(self, template):
        vmpool = self.one.vmpool.info(-4, -1, -1, -1)
        return [vm for vm in vmpool.VM if vm.TEMPLATE['TEMPLATE_ID'] == str(template)]

    def vm_ip(self, template_id, virtual_network_id, vm_id=None):
        # TODO: Clunky function...
        vms = self.vm_by_template(template_id)
        if vm_id is not None:
            vms = [vm for vm in vms if vm.ID == vm_id]
        if len(vms) > 1:
            logger.warning('More than one VM found with template ID: {}{}'.format(
                template_id, ' and ID {}'.format(vm_id) if vm_id is not None else ''))
        if len(vms) == 0:
            raise EcloudError('No running VM with template ID: {}'.format(template_id))
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    parser.add_argument('-r', '--reset', action='store_true')
    args = parser.parse_args()

    server = OpenNebulaServer(qos=args.qos, reset=args.reset)
    server.start()
