# -*- coding: utf-8 -*-
import boto.kinesis
import boto.kinesis.layer1
import uuid

from beaver.transports.base_transport import BaseTransport
from beaver.transports.exception import TransportException

from retrying import retry


class KinesisTransport(BaseTransport):

    def __init__(self, beaver_config, logger=None):
        super(KinesisTransport, self).__init__(beaver_config, logger=logger)

        self._access_key = beaver_config.get('kinesis_aws_access_key')
        self._secret_key = beaver_config.get('kinesis_aws_secret_key')
        self._region = beaver_config.get('kinesis_aws_region')
        self._stream_name = beaver_config.get('kinesis_aws_stream')

        # self-imposed max batch size to minimize the number of records in a given call to Kinesis
        self._batch_size_max = int(beaver_config.get('kinesis_aws_batch_size_max', '512000'))
        self._max_retries = int(beaver_config.get('kinesis_max_retries', '3'))
        self._initial_wait_between_retries = int(beaver_config.get('kinesis_initial_backoff_millis', '10'))

        # Kinesis Limit http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html#API_PutRecords_RequestSyntax
        self._max_records_per_batch = 500

        try:
            if self._access_key is None and self._secret_key is None:
                self._connection = boto.kinesis.connect_to_region(self._region)
            else:
                self._connection = boto.kinesis.connect_to_region(self._region,
                                                                  aws_access_key_id=self._access_key,
                                                                  aws_secret_access_key=self._secret_key)

            if self._connection is None:
                self._logger.warn('Unable to connect to AWS Kinesis - check your AWS credentials')
                raise TransportException('Unable to connect to AWS Kinesis - check your AWS credentials')

        except Exception, e:
            raise TransportException(e.message)

    def callback(self, filename, lines, **kwargs):
        timestamp = self.get_timestamp(**kwargs)
        if kwargs.get('timestamp', False):
            del kwargs['timestamp']

        message_batch = []
        message_batch_size = 0

        for line in lines:

            m = self.format(filename, line, timestamp, **kwargs)
            message_size = len(m)

            if (message_size > self._batch_size_max):
                self._logger.debug('Dropping the message as it is too large to send ({0} bytes)'.format(
                    message_size))
                continue

            # Check the self-enforced/declared batch size and flush before moving forward if we've eclipsed the max
            if len(message_batch) > 0 and ((message_batch_size + message_size) >= self._batch_size_max or len(message_batch) == self._max_records_per_batch):
                self._logger.debug('Flushing {0} messages to Kinesis stream {1} bytes'.format(
                    len(message_batch), message_batch_size))
                self._send_message_batch(message_batch)
                message_batch = []
                message_batch_size = 0

            message_batch_size = message_batch_size + message_size
            message_batch.append({'PartitionKey': uuid.uuid4().hex, 'Data': 
                self.format(filename, line, timestamp, **kwargs)})

        if len(message_batch) > 0:
            self._logger.debug('Flushing the last {0} messages to Kinesis stream {1} bytes'.format(
                len(message_batch), message_batch_size))
            self._send_message_batch(message_batch)

        return True

    def _send_message_batch(self, message_batch):
        @retry(wait_exponential_multiplier=self._initial_wait_between_retries,
               stop_max_attempt_number=self._max_retries,
               retry_on_exception=lambda exc: ('ProvisionedThroughputExceededException' in exc.message
                                               or 'Throttle' in exc.message or 'Throttling' in exc.message),
               retry_on_result=lambda res: res.get('FailedRecordCount', 0) > 0)
        def internal_send_message_batch_with_retry():
            return self._connection.put_records(records=message_batch, stream_name=self._stream_name)

        try:
            internal_send_message_batch_with_retry()
        except Exception, e:
            self._logger.exception('Exception occurred sending records to Kinesis stream')
            raise TransportException(e.message)

    def interrupt(self):
        return True

    def unhandled(self):
        return True
