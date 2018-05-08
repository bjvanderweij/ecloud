import unittest
from ecloud.ecdb import Task, TaskResult, FileTransfer, Worker, Context
from pony.orm import db_session
from ecloud.ecloud import *

class TestAPI(unittest.TestCase):

    @db_session()
    def setUp(self):


        self.one = get_one_server()

        # WARNING WARNING WARNING
        all = lambda obj: True
        Task.select(all).delete()
        Worker.select(all).delete()
        #Context.select(all).delete()

        self.task_0 = {'cmd':['echo Hello'], 'requires':['trade_secrets', ('temp/big_data', 'big_data')], 'results':['answers.txt'], 'key':'0'}
        self.task_1 = {'cmd':['echo Bye'], 'results':[], 'requires':['answers.txt', ('temp/big_data', 'big_data')], 'depends-on':['0'], 'key':'1'}

    @db_session()
    def test_get_boss_address(self):

        address = get_boss_address(self.one)
        self.assertIsNotNone(address)
        context = Context.get(key='boss_address')
        self.assertIsNotNone(context)
        self.assertEqual(context.value, address)

    @db_session()
    def test_instantiate_worker(self):

        address = get_boss_address(self.one)
        worker_id = instantiate_worker(self.one, address)
        workers = get_vms_by_template(self.one, str(settings.WORKER_TEMPLATE))
        self.assertIn(worker_id, [str(w.ID) for w in workers])
        worker = Worker.get(worker_id=worker_id)
        self.assertIsNotNone(worker)
        #self.delete_worker(worker_id)
        #worker = Worker.get(worker_id=worker_id)
        #self.assertIsNone(worker)

    @db_session()
    def test_queue_task(self):

        queue_task(self.task_0)
        task = Task.get(task_id='0')
        self.assertIsNotNone(task)
        self.assertEqual(task.status, Task.QUEUED)

    @db_session()
    def test_get_available_tasks(self):

        queue_task(self.task_0)
        queue_task(self.task_1)
        available = list(get_available_tasks())
        self.assertEqual(len(available), 1)
        self.assertEqual(available[0]['key'], '0')

    @db_session()
    def test_assign_worker_address(self):

        create_worker('0')
        worker = Worker.get(worker_id='0')
        self.assertIsNotNone(worker)
        self.assertEqual(worker.ip, '')
        assign_worker_address(worker.worker_id, '1.1.1.1')
        self.assertEqual(worker.ip, '1.1.1.1')
    
    @db_session()
    def test_assign_task(self):

        queue_task(self.task_0)
        create_worker('0')
        address = get_boss_address(self.one)
        assign_task('0', '0', address)
        task = Task.get(task_id='0')
        self.assertEqual(task.worker.worker_id, '0')
        self.assertEqual(task.status, Task.IN_PROGRESS)

    @db_session()
    def test_fail_task(self):
        queue_task(self.task_0)
        queue_task(self.task_1)
        create_worker('0')
        address = get_boss_address(self.one)
        assign_task('0', '0', address)
        finish_task('0', 1)
        self.assertEqual(Task.get(task_id='0').status, Task.FINISHED)
        self.assertTrue(Task.get(task_id='1').has_failed)
        self.assertFalse(Task.get(task_id='1').available)

    @db_session()
    def test_finish_task(self):
        queue_task(self.task_0)
        queue_task(self.task_1)
        create_worker('0')
        address = get_boss_address(self.one)
        assign_task('0', '0', address)
        finish_task('0', 0)
        self.assertEqual(Task.get(task_id='0').status, Task.FINISHED)
        self.assertTrue(Task.get(task_id='1').available)
        self.assertFalse(Task.get(task_id='1').has_failed)

if __name__ == '__main__':
    unittest.main()
