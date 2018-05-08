import argparse, settings

from pony.orm import db_session, select

from ecloud.ecdb import Task
from ecloud.ecloud import get_workers

def get_in_progress():
    return list(Task.select(lambda t: t.status == Task.IN_PROGRESS))

def get_queued():
    return list(Task.select(lambda t: t.status == Task.QUEUED))

def get_available():
    return list(t for t in Task.select(lambda t: t.status == Task.QUEUED) if t.available)

def get_finished():
    return list(Task.select(lambda t: t.status == Task.FINISHED))

@db_session()
def print_status():
    workers = get_workers()
    tasks = get_queued()
    available = get_available()
    in_progress = get_in_progress()
    finished = get_finished()
    succeeded = [t for t in finished if t.result.success]
    failed = [t for t in finished if not t.result.success]
    print('%d workers created.' % len(workers))
    print('%d tasks in queue of which %d are currently available.' % (len(tasks), len(available)))
    print('%d tasks in progress.' % len(in_progress))
    print('%d tasks finished. %d succeeded and %d failed.' % (len(finished), len(succeeded), len(failed)))

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    args = parser.parse_args()

    print_status()

