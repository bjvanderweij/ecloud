import argparse, pyone, settings, ecloud

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('worker_id')
    parser.add_argument('init_exit', type=int)
    parser.add_argument('task_exit', type=int)
    parser.add_argument('finalize_exit', type=int)

    args = parser.parse_args()

    one = ecloud.get_one_server()
    ecloud.finish_task(args.worker_id, args.init_exit, args.task_exit, args.finalize_exit)
    if any(e != 0 for e in (args.init_exit, args.task_exit, args.finalize_exit)):
        ecloud.mark_failed_tasks()
