# ~*~ encoding: utf-8 ~*~
import sys
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

from mock import Mock
import time
import threading

from beaver.worker.tail_manager import ConsumerManager


class TestConsumerManager(unittest.TestCase):
    def test_basic_run(self):
        queue_consumer_func = Mock()
        manager = ConsumerManager(queue_consumer_func, number_of_consumer_processes=1, interval=0.1)
        thread = threading.Thread(target=manager.run)
        thread.daemon = True
        manager.join = lambda *args, **kwargs: thread.join(*args, **kwargs)

        # Make it appear as if the first process fails to see a reviver run happen.
        queue_consumer_func.return_value.is_alive.side_effect = [False, True]

        try:
            thread.start()
            # 2 calls, once to originally fire up the consumer, and again when it sees that the
            # first consumer it started is not alive!
            self._assert_eventually(lambda: queue_consumer_func.call_count == 2, 2.0)
        finally:
            manager.stop(2.0)
            # Graceful shutdown por favor.
            self.assertFalse(thread.is_alive())

    def _assert_eventually(self, condition, timeout):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if condition():
                return True
            time.sleep(0.1)
        self.fail("Timeout out waiting for condition")
