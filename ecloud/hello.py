import argparse, pyone, settings
from ecloud.ecloud import get_worker_id_by_ip, assign_worker_address

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('worker_ip')

    args = parser.parse_args()

    one = pyone.OneServer(settings.ONE_API_ENDPOINT, session="%s:%s" % (settings.ONE_USERNAME, settings.ONE_PASSWORD))
    worker_id = get_worker_id_by_ip(one, args.worker_ip)

    # Only interact with workers we created
    if worker_id is not None:
        assign_worker_address(worker_id, args.worker_ip)
        # Send the ID back to the worker who called this method
        print(worker_id)
    elif settings.DEBUG:
        print('Not interacting with unknown worker.')



