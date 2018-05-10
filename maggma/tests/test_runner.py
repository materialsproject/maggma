# coding: utf-8
"""
Tests for the Runner class
"""
import unittest
from unittest.mock import patch, MagicMock

from maggma.runner import Runner, SerialProcessor, MultiprocProcessor, MPIProcessor

__author__ = 'Shyam Dwaraknath'
__email__ = 'shyamd@lbl.gov'


class TestRunner(unittest.TestCase):
    def test_1(self):
        builder1 = MagicMock()
        builder2 = MagicMock()

        builder1.configure_mock(sources=[1, 2, 3], targets=[4])
        builder2.configure_mock(sources=[3, 4, 5], targets=[6])
        self.builders = [builder1, builder2]

        rnr = Runner(self.builders)
        self.assertEqual(rnr.dependency_graph, {1: [0]})


class TestSerialProcessor(unittest.TestCase):
    def test_process(self):

        builder = MagicMock()
        builder.configure_mock(chunk_size=10)
        builder.get_items.return_value = range(10)
        builder.process_item.side_effect = range(10, 20)

        proc = SerialProcessor([builder])

        proc.process(0)

        builder.get_items.assert_called()
        self.assertEqual(builder.process_item.call_count, 10)
        builder.update_targets.assert_called()


class TestMultiprocProcessor(unittest.TestCase):
    def setUp(self):
        builder = MagicMock()
        builder.configure_mock(chunk_size=10)
        builder.get_items.return_value = iter(range(10))
        builder.process_item.side_effect = range(10, 20)
        builder.from_dict.return_value = {}

        self.builder = builder

    def test_init(self):
        proc = MultiprocProcessor([], 3)
        self.assertEqual(proc.num_workers, 3)

    def test_setup_multithreading(self):
        
        with patch("maggma.runner.Thread") as mock_thread:
            proc = MultiprocProcessor([self.builder], num_workers=3)
            proc.builder = proc.builders[0]
            proc.setup_multithreading()
            mock_thread.assert_called()

    def test_update_targets(self):

        proc = MultiprocProcessor([self.builder], num_workers=3)

        proc.builder = self.builder
        proc.update_data_condition = MagicMock()
        proc.data = MagicMock()
        proc.update_pbar = MagicMock()
        proc.run_update_targets = MagicMock()
        proc.run_update_targets.__bool__.side_effect = [True,True,True,False]

        proc.update_targets()
        proc.run_update_targets.__bool__.assert_called()
        proc.data.clear.assert_called()
        proc.update_data_condition.wait_for.assert_called()
        proc.builder.update_targets.assert_called()

    def test_update_data_callback(self):

        proc = MultiprocProcessor([self.builder], num_workers=3)

        future = MagicMock()
        proc.data = MagicMock()
        proc.task_count = MagicMock()
        proc.update_data_condition = MagicMock()
        proc.process_pbar = MagicMock()
        proc.update_data_callback(future)

        future.result.assert_called()
        proc.update_data_condition.notify_all.assert_called()
        proc.task_count.release.assert_called()

    def test_clean_up_data(self):

        proc = MultiprocProcessor([self.builder], num_workers=3)

        proc.data = MagicMock()
        proc.update_data_condition = MagicMock()
        proc.builder = MagicMock()
        proc.update_targets_thread = MagicMock()

        proc.clean_up_data()

        proc.update_data_condition.notify_all.assert_called()
        proc.update_targets_thread.join.assert_called()

    def test_put_tasks(self):

        with patch("maggma.runner.ProcessPoolExecutor") as mock_executor:

            mock_exec_obj = mock_executor()
            proc = MultiprocProcessor([self.builder], num_workers=3)
            proc.builder = MagicMock()
            proc.task_count = MagicMock()
            cursor = [True,True,True,False]
            proc.get_pbar = cursor
            proc.put_tasks()
            proc.task_count.acquire.assert_called()


class TestMPIProcessor(unittest.TestCase):
    def setUp(self):
        builder = MagicMock()
        builder.configure_mock(chunk_size=10)
        builder.get_items.return_value = iter(range(10))
        builder.process_item.side_effect = range(10, 20)
        builder.from_dict.return_value = {}
        builder.setup_pbars([])

        self.builder = builder
        self.get_mpi_patcher = patch("maggma.runner.get_mpi")
        self.get_mpi = self.get_mpi_patcher.start()
        self.comm = MagicMock()
        self.get_mpi.return_value = self.comm, 0, 2  # comm, rank , size

    def tearDown(self):
        self.get_mpi.stop()

    def test_init(self):
        proc = MPIProcessor([self.builder])
        self.comm.barrier.assert_called()

    def test_setup_multithreading(self):

        with patch("maggma.runner.Thread") as mock_thread:
            proc = MPIProcessor([self.builder])
            proc.builder = proc.builders[0]
            proc.setup_multithreading()
            mock_thread.assert_called()

    def test_update_targets(self):

        proc = MPIProcessor([self.builder])

        proc.builder = self.builder
        proc.update_data_condition = MagicMock()
        proc.data = MagicMock()
        proc.run_update_targets = MagicMock()
        proc.run_update_targets.__bool__.side_effect = [True,True,True,False]
        proc.setup_pbars([])

        proc.update_targets()
        proc.run_update_targets.__bool__.assert_called()
        proc.data.clear.assert_called()
        proc.update_data_condition.wait_for.assert_called()
        proc.builder.update_targets.assert_called()

    def test_clean_up_data(self):

        proc = MPIProcessor([self.builder])

        proc.data = MagicMock()
        proc.update_data_condition = MagicMock()
        proc.builder = MagicMock()
        proc.update_targets_thread = MagicMock()

        proc.clean_up_data()

        proc.update_data_condition.notify_all.assert_called()
        proc.update_targets_thread.join.assert_called()

    def test_clean_up_workers(self):
        proc = MPIProcessor([self.builder])

        proc.clean_up_workers()
        self.comm.send.assert_called()
        self.assertEqual(self.comm.send.call_count, 1)

    def test_submit_item(self):

        proc = MPIProcessor([self.builder])
        proc.ranks = MagicMock()
        proc.update_data_condition = MagicMock()
        proc.data = MagicMock()
        proc.task_count = MagicMock()
        proc.setup_pbars([])
        self.comm.recv.return_value = {"type": "return", "return": "data"}

        proc.submit_item(0, {})

        self.comm.recv.assert_called()
        proc.update_data_condition.__enter__.assert_called()
        proc.data.append.assert_called()
        proc.update_data_condition.notify_all.assert_called()

        proc.ranks.append.assert_called()

    def test_put_tasks(self):
        with patch("maggma.runner.ThreadPoolExecutor") as mock_executor:
            proc = MPIProcessor([self.builder])
            proc.builder = MagicMock()
            proc.task_count = MagicMock()
            cursor = [True,True,True,False]


            proc.setup_pbars(cursor)
            proc.put_tasks(0)
            proc.task_count.acquire.assert_called()
            mock_executor.return_value.__enter__.assert_called()
            proc.task_count.acquire.assert_called()
            mock_executor.return_value.__enter__.return_value.submit.assert_called()

    def test_proccess_worker(self):
        proc = MPIProcessor([self.builder])

        self.comm.recv.side_effect = [{"type": "process", "builder_id": 0, "data": ""}, {"type": "shutdown"}]

        proc.process_worker()

        self.comm.recv.assert_called()
        self.comm.send.assert_called()
        self.builder.process_item.assert_called()


if __name__ == "__main__":
    unittest.main()
