# -*- coding: utf-8 -*-
import os.path
from beaver.transports.base_transport import BaseTransport


class FileTransport(BaseTransport):
    """
    Just writes all messages to a file. Mostly used for integration tests.
    """
    def __init__(self, beaver_config, logger=None):
        super(FileTransport, self).__init__(beaver_config, logger=logger)
        self._filepath = beaver_config.get('file_transport_output_path')
        if not self._filepath:
            raise ValueError('Missing output filepath. Please set file_transport_output_path in the config.')
        if not os.path.exists(os.path.dirname(self._filepath)):
            raise ValueError('Directory {} does not exist'.format(os.path.dirname(self._filepath)))

    def callback(self, filename, lines, **kwargs):
        timestamp = self.get_timestamp(**kwargs)
        if kwargs.get('timestamp', False):
            del kwargs['timestamp']

        with open(self._filepath, 'a') as f:
            for line in lines:
                f.write(self.format(filename, line, timestamp, **kwargs))
                f.write('\n')
                f.flush()
