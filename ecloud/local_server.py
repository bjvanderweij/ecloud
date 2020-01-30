from server import Server
from worker import start_worker
import argparse, settings, multiprocessing as mp

class LocalServer(Server):

    processes = {}

    def instantiate(self, worker):
        ... # launch child process?
        # Launch child process, keep record, and detach
        p = mp.Process(target=start_worker, args=(worker.id, self.broker_url, self.broker_port))
        p.start()
        self.processes[worker.id] = p

    def terminate_worker(self, worker):
        self.processes[worker.id].terminate()
        del self.processes[worker.id]

    def find_results(self):
        find = ['find',  settings.LOCAL_DATASTORE_DIR, '-printf', '\'%P\n\'']
        return self.exec(find).stdout.decode().split('\n')

if __name__ == '__main__':
    mp.set_start_method('spawn') # is this the best solution? https://docs.python.org/3.7/library/multiprocessing.html#module-multiprocessing
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    parser.add_argument('-r', '--reset', action='store_true')
    parser.add_argument('--broker_url', default=settings.BROKER_URL)
    args = parser.parse_args()

    server = LocalServer(qos=args.qos, broker_url=args.broker_url)
    server.start()
