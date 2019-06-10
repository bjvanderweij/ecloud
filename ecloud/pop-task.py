import argparse, settings, sys 
import ecloud

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('worker_id')
    parser.add_argument('--nojson')

    args = parser.parse_args()

    one = ecloud.get_one_server()
    worker = ecloud.get_worker(args.worker_id)

    # Only interact with workers we created
    if worker is not None:
        task = ecloud.get_available_task()
        if task is None and not worker.keep_alive:
            print('I WILL TERMINATE YOU')
            ecloud.terminate_worker(one, worker.worker_id)
        else:
            boss_address = ecloud.get_boss_address(one)
            print(task.task_id)
            ecloud.assign_task(worker.worker_id, task.task_id, boss_address)

    elif settings.DEBUG:
        print('Not interacting with unknown worker.')
