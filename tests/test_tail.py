# ~*~ encoding: utf-8 ~*~
import sys
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

import mock
import tempfile

from beaver.config import BeaverConfig
from beaver.worker.tail import  Tail
import sqlite3

class TestTail(unittest.TestCase):

    def setUp(self):
        self.sincedb_path = tempfile.NamedTemporaryFile(delete=True).name
        self.filename = tempfile.NamedTemporaryFile(delete=True).name

        beaver_config = self._get_config()
        beaver_config.set('sincedb_path', self.sincedb_path)
        self.tail = Tail(self.filename, None, beaver_config=beaver_config)

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
