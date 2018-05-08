import argparse, json, pyone, os, settings, time
import ecloud.ecloud
from hashlib import md5

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('tasks_file')
    parser.add_argument('--create_workers', action='store_true')
    parser.add_argument('--drop_workers', action='store_true')
    parser.add_argument('--drop_tasks', action='store_true')
    parser.add_argument('--drop_context', action='store_true')
    parser.add_argument('--datastore', type=str, help='The address of the datastore.')
    parser.add_argument('--merge_strategy', choices=['replace', 'keep_succeeded'], default=None)
    parser.add_argument('--skip_existing', action='store_true', help='If the results of a task are found in the datastore, mark the task finished and succeeded.')
    parser.add_argument('--create_ids', action='store_true')

    args = parser.parse_args()

    one = pyone.OneServer(settings.ONE_API_ENDPOINT, session="%s:%s" % (settings.ONE_USERNAME, settings.ONE_PASSWORD))

    def make_key(task):
        identifier = '%s%s%s%s%s' % (time.time(), task['results'], task['cmd'], ''.join(task.get('requires', [])), ''.join(task.get('depends_on', [])))
        return md5(identifier.encode()).hexdigest()[:10]

    with open(args.tasks_file) as tasks_file:
        tasks = json.loads(tasks_file.read())

    if args.create_ids:
        for task in tasks:
            task['key'] = make_key(task)

    if args.drop_workers:
        print('Dropping workers.')
        ecloud.drop_workers()

    if args.drop_tasks:
        print('Dropping tasks.')
        ecloud.drop_tasks()

    if args.drop_context:
        print('Dropping context.')
        ecloud.drop_context()

    if args.datastore is not None:
        ecloud.set_datastore_address(args.datastore)

    if ecloud.get_datastore_address() is None:
        print('WARNING: datastore address not set, assuming boss address (set your own with --datastore).')
        boss_address = ecloud.get_boss_address(one)
        ecloud.set_datastore_address(boss_address)

    task_dict = {}
    for task in tasks:
        task_dict[task['key']] = task

    print('Queueing tasks.')
    existing_results = ecloud.get_datastore_file_tree(one)
    for task in tasks:
        results_exist = all(r in existing_results for r in task['results'])
        if args.skip_existing and results_exist:
            ecloud.add_as_succeeded(task)
        else:
            ecloud.queue_task(task, merge_strategy=args.merge_strategy)

    if args.create_workers:
        boss_address = ecloud.get_boss_address(one)
        if boss_address is None:
            print('Failed to find boss address. Try again later.')
        else:
            ecloud.fill_worker_pool(one, boss_address)
