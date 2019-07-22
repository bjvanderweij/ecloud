from server import Server
import argparse, settings

class LocalServer(Server):

    def instantiate(self, worker):
        ... # launch child process?

    def find_results(self):
        find = ['find',  settings.LOCAL_DATASTORE_DIR, '-printf', '\'%P\n\'']
        return self.exec(find).stdout.decode().split('\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--qos', type=int, choices=[0,1,2], default=1)
    parser.add_argument('-r', '--reset', action='store_true')
    args = parser.parse_args()

    server = LocalServer(qos=args.qos)
    server.start()
