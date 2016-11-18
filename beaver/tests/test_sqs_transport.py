# -*- coding: utf-8 -*-
import sys
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

import mock
import multiprocessing
import tempfile
import logging
import time
from threading import Thread

import beaver
from beaver.config import BeaverConfig
from beaver.run_queue import run_queue
from beaver.transports import create_transport
from beaver.transports.exception import TransportException
from beaver.transports.sqs_transport import SqsTransport
from beaver.unicode_dammit import unicode_dammit

from fixtures import Fixture

from moto import mock_sqs
import boto.sqs

class SqsTests(unittest.TestCase):

    @mock_sqs
    def _create_queues(self):
        conn = boto.sqs.connect_to_region("us-east-1")
        conn.create_queue("queue1")
        conn.create_queue("queue2")

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger(__name__)

        empty_conf = tempfile.NamedTemporaryFile(delete=True)
        cls.beaver_config = BeaverConfig(mock.Mock(config=empty_conf.name))
        cls.beaver_config.set('transport', 'sqs')
        cls.beaver_config.set('logstash_version', 1)

        output_file = Fixture.download_official_distribution()
        Fixture.extract_distribution(output_file)

    @mock_sqs
    def test_sqs_default_auth_profile(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_aws_queue', 'queue1')

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)
        transport.interrupt()

    @mock_sqs
    def test_sqs_auth_profile(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_profile_name', 'beaver_queue')
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_aws_queue', 'queue1')

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)

    @mock_sqs
    def test_sqs_auth_key(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', 'beaver_test_key')
        self.beaver_config.set('sqs_aws_secret_key', 'beaver_test_secret')
        self.beaver_config.set('sqs_aws_queue', 'queue1')

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)
        transport.interrupt()

    @mock_sqs
    def test_sqs_auth_account_id(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue_owner_acct_id', 'abc123')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', 'beaver_test_key')
        self.beaver_config.set('sqs_aws_secret_key', 'beaver_test_secret')
        self.beaver_config.set('sqs_aws_queue', 'queue1')

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)
        transport.interrupt()

    @mock_sqs
    def test_sqs_single_queue(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)
        transport.interrupt()

    @mock_sqs
    def test_sqs_single_queue_bulklines(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_bulk_lines', True)

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)
        transport.interrupt()

    @mock_sqs
    def test_sqs_multi_queue(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1,queue2')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_bulk_lines', False)

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)
        transport.interrupt()

    @mock_sqs
    def test_sqs_multi_queue_bulklines(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1,queue2')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_bulk_lines', True)

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)
        transport.interrupt()

    @mock_sqs
    def test_sqs_send_single_queue(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_bulk_lines', False)

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)

        data = {}
        lines = []
        n=100
        for i in range(n):
            lines.append('log' + str(i) + '\n')
        new_lines = []
        for line in lines:
            message = unicode_dammit(line)
            if len(message) == 0:
                continue
            new_lines.append(message)
        data['lines'] = new_lines
        data['fields'] = []
        transport.callback("test.log", **data)

    @mock_sqs
    def test_sqs_send_multi_queue(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1,queue2')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_bulk_lines', False)

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)

        data = {}
        lines = []
        n=100
        for i in range(n):
            lines.append('log' + str(i) + '\n')
        new_lines = []
        for line in lines:
            message = unicode_dammit(line)
            if len(message) == 0:
                continue
            new_lines.append(message)
        data['lines'] = new_lines
        data['fields'] = []
        transport.callback("test.log", **data)

    @mock_sqs
    def test_sqs_send_single_queue_bulklines(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_bulk_lines', True)

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)

        data = {}
        lines = []
        n=100
        for i in range(n):
            lines.append('log' + str(i) + '\n')
        new_lines = []
        for line in lines:
            message = unicode_dammit(line)
            if len(message) == 0:
                continue
            new_lines.append(message)
        data['lines'] = new_lines
        data['fields'] = []
        transport.callback("test.log", **data)

    @mock_sqs
    def test_sqs_send_multi_queue_bulklines(self):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1,queue2')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_bulk_lines', True)

        transport = create_transport(self.beaver_config, logger=self.logger)

        self.assertIsInstance(transport, beaver.transports.sqs_transport.SqsTransport)

        data = {}
        lines = []
        n=100
        for i in range(n):
            lines.append('log' + str(i) + '\n')
        new_lines = []
        for line in lines:
            message = unicode_dammit(line)
            if len(message) == 0:
                continue
            new_lines.append(message)
        data['lines'] = new_lines
        data['fields'] = []
        transport.callback("test.log", **data)

    @mock_sqs
    @mock.patch.object(SqsTransport, '_send_message_batch')
    def test_sqs_send_batch_queue_with_retries(self, mock_send_batch):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1,queue2')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_bulk_lines', False)
        self.beaver_config.set('respawn_delay', 0)

        mock_send_batch.side_effect = [TransportException(), True]

        data = {}
        lines = []
        n = 1
        for i in range(n):
            lines.append('log' + str(i) + '\n')
        new_lines = []
        for line in lines:
            message = unicode_dammit(line)
            if len(message) == 0:
                continue
            new_lines.append(message)
        data['lines'] = new_lines
        data['fields'] = []
        data['filename'] = 'test.log'
        queue = multiprocessing.JoinableQueue(10)
        queue.put(('callback', data))

        with mock.patch('signal.signal'):
            thread = Thread(target=run_queue, args=(queue, self.beaver_config),
                            kwargs={'logger': self.logger})
            thread.daemon = True # So that it exits when the tests do
            thread.start()
            time.sleep(1)
            self.assertEqual(2, mock_send_batch.call_count)
            queue.put(('exit', {}))


    @mock_sqs
    @mock.patch.object(SqsTransport, '_send_message')
    def test_sqs_send_batch_queue_with_retries(self, mock_send):
        self._create_queues()
        self.beaver_config.set('sqs_aws_queue', 'queue1,queue2')
        self.beaver_config.set('sqs_aws_profile_name', None)
        self.beaver_config.set('sqs_aws_access_key', None)
        self.beaver_config.set('sqs_aws_secret_key', None)
        self.beaver_config.set('sqs_bulk_lines', True)
        self.beaver_config.set('respawn_delay', 0)

        mock_send.side_effect = [TransportException(), True]

        data = {}
        lines = []
        n = 1
        for i in range(n):
            lines.append('log' + str(i) + '\n')
        new_lines = []
        for line in lines:
            message = unicode_dammit(line)
            if len(message) == 0:
                continue
            new_lines.append(message)
        data['lines'] = new_lines
        data['fields'] = []
        data['filename'] = 'test.log'
        queue = multiprocessing.JoinableQueue(10)
        queue.put(('callback', data))

        with mock.patch('signal.signal'):
            thread = Thread(target=run_queue, args=(queue, self.beaver_config),
                            kwargs={'logger': self.logger})
            thread.daemon = True # So that it exits when the tests do
            thread.start()
            time.sleep(1)
            self.assertEqual(2, mock_send.call_count)
            queue.put(('exit', {}))
