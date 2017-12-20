# ~*~ encoding: utf-8 ~*~
import sys
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

import mock
import tempfile
import time
from datetime import datetime

from beaver.config import BeaverConfig
from beaver.worker.tail import  Tail
import sqlite3

class TestTail(unittest.TestCase):

    def setUp(self):
        self.sincedb_path = tempfile.NamedTemporaryFile(delete=True).name
        self.filename = tempfile.NamedTemporaryFile(delete=True).name

        self.beaver_config = self._get_config()
        self.beaver_config.set('sincedb_path', self.sincedb_path)
        self.callback = mock.Mock()
        self.tail = Tail(self.filename, self.callback, beaver_config=self.beaver_config)

    def _get_config(self, **kwargs):
        empty_conf = tempfile.NamedTemporaryFile(delete=True)
        return BeaverConfig(mock.Mock(config=empty_conf.name, **kwargs))

    def _test_close_parameterized_remove_db_entry(self, remove_db_entry):
        # Fid is needed but instantiated during a run, so we 'mock'.
        self.tail._fid = 'fid'
        # Update the position so we have an entry.
        self.assertTrue(self.tail._sincedb_update_position(20, force_update=True))
        # active is also set during a run, but we must force it.
        self.tail.active = True

        # close with the parameterized remove_db_entry and return the number of results.
        self.tail.close(remove_db_entry=remove_db_entry)
        conn = sqlite3.connect(self.sincedb_path, isolation_level=None)
        cursor = conn.cursor()
        cursor.execute('select * from sincedb where filename = :filename', {
            'filename': self.filename
        })
        return cursor.fetchall()

    def test_close_with_remove_db_entry(self):
        # With remove entry, results should be 0 since db entry is removed.
        results = self._test_close_parameterized_remove_db_entry(True)
        self.assertEquals(len(results), 0)

    def test_close_without_remove_db_entry(self):
        # Without remove entry, results should be 1.
        results = self._test_close_parameterized_remove_db_entry(False)
        self.assertEquals(len(results), 1)

    def test_runtail_defaults(self):
        self.tail._update_file()
        lines = ['test', 'test2']
        with open(self.filename, 'a') as logfile:
            # Create the file
            logfile.write('')
            # Update the file tracking internally
            self.tail._update_file()
            # Write to the file
            logfile.write('\n'.join(lines) + '\n')

        start_time = time.time()
        self.tail._sincedb_update_position = mock.Mock(wraps=self.tail._sincedb_update_position)
        self.tail._run_pass()
        # Should be super fast
        self.assertGreaterEqual(0.1, time.time() - start_time)

        self.callback.assert_called_once()
        self.assertEqual(lines, self.callback.call_args[0][0][1]['lines'])
        self.assertEqual(2, self.tail._sincedb_update_position.call_count)

        # check sincedb is correct
        self.tail._sincedb_update_position(force_update=True)
        self.assertEqual(2, self.tail._sincedb_start_position())

    def test_runtail_nolines(self):
        with open(self.filename, 'a') as logfile:
            # Create the file
            logfile.write('')
            # Update the file tracking internally
            self.tail._update_file()

        self.tail._sincedb_update_position = mock.Mock(wraps=self.tail._sincedb_update_position)
        start_time = time.time()
        self.tail._run_pass()
        # Should be super fast
        self.assertGreaterEqual(0.1, time.time() - start_time)
        self.callback.assert_not_called()
        self.tail._sincedb_update_position.assert_called_once()

        # check sincedb is correct
        self.tail._sincedb_update_position(force_update=True)
        self.assertEqual(0, self.tail._sincedb_start_position())

    def test_runtail_with_flush_buffer_lines(self):
        self.tail._update_file()
        lines = ['test', 'test2']
        flush_seconds = 1
        self.beaver_config.set('buffered_lines_max_lines', 2)
        self.beaver_config.set('buffered_lines_max_bytes', 10000)
        self.beaver_config.set('buffered_lines_max_seconds', flush_seconds)

        self.tail = Tail(self.filename, self.callback, beaver_config=self.beaver_config)

        with open(self.filename, 'a') as logfile:
            # Create the file
            logfile.write('')
            # Update the file tracking internally
            self.tail._update_file()
            # Write to the file
            logfile.write('\n'.join(lines) + '\n')

        self.tail._sincedb_update_position = mock.Mock(wraps=self.tail._sincedb_update_position)
        start_time = datetime.utcnow()
        self.tail._run_pass()
        # Should be super fast
        called_time = datetime.strptime(self.callback.call_args[0][0][1]['timestamp'],
                                        '%Y-%m-%dT%H:%M:%S.%fZ')
        self.assertLessEqual((called_time - start_time).total_seconds(), 0.1)

        self.callback.assert_called_once()
        self.assertEqual(lines, self.callback.call_args[0][0][1]['lines'])

        self.callback.reset_mock()
        lines = ['test3']
        with open(self.filename, 'a') as logfile:
            # Write to the file
            logfile.write('\n'.join(lines) + '\n')

        start_time = datetime.utcnow()
        self.tail._run_pass()
        called_time = datetime.strptime(self.callback.call_args[0][0][1]['timestamp'],
                                        '%Y-%m-%dT%H:%M:%S.%fZ')
        self.assertLessEqual(flush_seconds, (called_time - start_time).total_seconds())
        self.assertEqual(3, self.tail._sincedb_update_position.call_count)

        # check sincedb is correct
        self.tail._sincedb_update_position(force_update=True)
        self.assertEqual(3, self.tail._sincedb_start_position())

    def test_runntail_with_flush_buffer_bytes(self):
        self.tail._update_file()
        lines = ['test', 'test2']
        flush_seconds = 1
        self.beaver_config.set('buffered_lines_max_lines', 100)
        self.beaver_config.set('buffered_lines_max_bytes', 8)
        self.beaver_config.set('buffered_lines_max_seconds', flush_seconds)

        self.tail = Tail(self.filename, self.callback, beaver_config=self.beaver_config)

        with open(self.filename, 'a') as logfile:
            # Create the file
            logfile.write('')
            # Update the file tracking internally
            self.tail._update_file()
            # Write to the file
            logfile.write('\n'.join(lines) + '\n')

        self.tail._sincedb_update_position = mock.Mock(wraps=self.tail._sincedb_update_position)
        start_time = datetime.utcnow()
        self.tail._run_pass()
        self.callback.assert_called_once()
        self.assertEqual(lines, self.callback.call_args[0][0][1]['lines'])
        called_time = datetime.strptime(self.callback.call_args[0][0][1]['timestamp'],
                                        '%Y-%m-%dT%H:%M:%S.%fZ')
        self.assertLessEqual((called_time - start_time).total_seconds(), 0.1)

        self.callback.reset_mock()
        lines = ['a', 'b']
        with open(self.filename, 'a') as logfile:
            # Write to the file
            logfile.write('\n'.join(lines) + '\n')

        start_time = datetime.utcnow()
        self.tail._run_pass()
        called_time = datetime.strptime(self.callback.call_args[0][0][1]['timestamp'],
                                        '%Y-%m-%dT%H:%M:%S.%fZ')
        self.assertLessEqual(flush_seconds, (called_time - start_time).total_seconds())
        self.assertEqual(3, self.tail._sincedb_update_position.call_count)

        # check sincedb is correct
        self.tail._sincedb_update_position(force_update=True)
        self.assertEqual(4, self.tail._sincedb_start_position())
