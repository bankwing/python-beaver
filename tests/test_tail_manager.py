# ~*~ encoding: utf-8 ~*~
import sys
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

from mock import MagicMock

from beaver.worker.tail_manager import TailManager


class TestTailManager(unittest.TestCase):
    def test_create_queue_consumer_if_required_fails(self):
        mock_config = MagicMock()

        manager = TailManager(mock_config, lambda: None, lambda: None, logger=None)
        mock_create_queue_consumer = MagicMock()
        manager.create_queue_consumer_if_required = mock_create_queue_consumer
        manager.update_files = MagicMock()
        mock_create_queue_consumer.side_effect = RuntimeError('oops!')

        with self.assertRaises(RuntimeError) as context:
            manager.run(interval=0.01)

        self.assertEqual(1, mock_create_queue_consumer.call_count)
