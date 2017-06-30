import unittest
import time
from tempfile import NamedTemporaryFile
from multiprocessing import Process
import json

from beaver.dispatcher import tail
from beaver.utils import parse_args


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.input = NamedTemporaryFile()
        self.output = NamedTemporaryFile()

        # Set the config.
        self.beaver_config = NamedTemporaryFile()
        self.beaver_config.write("[beaver]\n"
                                 "logstash_version: 0\n"
                                 "file_transport_output_path: {}\n".format(self.output.name))
        self.beaver_config.flush()

    def tearDown(self):
        self.input.close()
        self.output.close()
        self.beaver_config.close()

    def _run_async(self, args=None):
        args = args or []
        beaver_args = parse_args(args +
                                 ['--files', self.input.name,
                                  '--transport', 'file',
                                  '--format', 'json',
                                  '--configfile', self.beaver_config.name])
        p = Process(target=tail.run, name='beaver_run', args=(beaver_args,))
        p.start()
        return p

    def _output_contains(self, lines, timeout_seconds=10):
        start = time.time()
        while time.time() - start < timeout_seconds:
            with open(self.output.name, 'r') as f:
                file_lines = [json.loads(line)['@message']
                              for line in f.readlines()]
                if len(file_lines) < len(lines):
                    continue
                elif len(file_lines) > len(lines):
                    return False
                elif lines == file_lines:
                    return True
            time.sleep(0.1)
        return False

    def test_run_basic(self):
        process = self._run_async()
        # Unfortunately the best way to let the process startup until we get
        # Better eventing in the code.
        time.sleep(1)
        try:
            line1 = "Test1"
            self.input.write(line1 + '\n')
            self.input.flush()
            self.assertTrue(self._output_contains([line1]))

            line2 = "Test2"
            self.input.write(line2 + '\n')
            self.input.flush()
            self.assertTrue(self._output_contains([line1, line2]))
        finally:
            process.terminate()

    def test_with_logging_config_yaml(self):
        logging_config = NamedTemporaryFile(suffix='.yaml')
        logger_output = NamedTemporaryFile()

        # create a basic logging config
        logging_config.write('\n'.join(["version: 1",
                                        "formatters:",
                                        "  file:",
                                        "    format: '%(asctime)s TESTING %(message)s'",
                                        "handlers:",
                                        "  file:",
                                        "    class: logging.handlers.WatchedFileHandler",
                                        "    filename: '{}'".format(logger_output.name),
                                        "    formatter: file",
                                        "    level: INFO",
                                        "root:",
                                        "  level: INFO",
                                        "  handlers:",
                                        "    - file"]))
        logging_config.flush()

        process = self._run_async(['--logging-config', logging_config.name])
        # Unfortunately the best way to let the process startup until we get
        # Better eventing in the code.
        time.sleep(1)
        try:
            line1 = "Test1"
            self.input.write(line1 + '\n')
            self.input.flush()
            self.assertTrue(self._output_contains([line1]))

            line2 = "Test2"
            self.input.write(line2 + '\n')
            self.input.flush()
            self.assertTrue(self._output_contains([line1, line2]))

            with open(logger_output.name, 'r') as logfile:
                lines = logfile.readlines()
                self.assertTrue(len(lines) > 0)
                for line in lines:
                    self.assertTrue('TEST' in line)
        finally:
            process.terminate()

    def test_with_logging_config_json(self):
        logging_config = NamedTemporaryFile(suffix='.json')
        logger_output = NamedTemporaryFile()

        # create a basic logging config
        logging_config.write(json.dumps({
            'version': 1,
            'formatters': {
                'file': {
                    'format': '%(asctime)s TESTING %(message)s',
                }
            },
            'handlers': {
                'file': {
                    'class': 'logging.handlers.WatchedFileHandler',
                    'filename': logger_output.name,
                    'formatter': 'file',
                    'level': 'INFO'
                }
            },
            'root': {
                'level': 'INFO',
                'handlers': ['file'],
            }
        }))
        logging_config.flush()

        process = self._run_async(['--logging-config', logging_config.name])
        # Unfortunately the best way to let the process startup until we get
        # Better eventing in the code.
        time.sleep(1)
        try:
            line1 = "Test1"
            self.input.write(line1 + '\n')
            self.input.flush()
            self.assertTrue(self._output_contains([line1]))

            line2 = "Test2"
            self.input.write(line2 + '\n')
            self.input.flush()
            self.assertTrue(self._output_contains([line1, line2]))

            with open(logger_output.name, 'r') as logfile:
                lines = logfile.readlines()
                self.assertTrue(len(lines) > 0)
                for line in lines:
                    self.assertTrue('TEST' in line)
        finally:
            process.terminate()

    def test_with_logging_config_with_ini(self):
        logging_config = NamedTemporaryFile(suffix='.cfg')
        logger_output = NamedTemporaryFile()

        # create a basic logging config
        logging_config.write('\n'.join([
            "[loggers]",
            "keys = root",

            "[handlers]",
            "keys = file",

            "[formatters]",
            "keys = file",

            "[formatter_file]",
            "format = %(asctime)s TESTING %(message)s",

            "[handler_file]",
            "class = logging.handlers.WatchedFileHandler",
            "formatter = file",
            "level = INFO",
            "args = ('{}',)".format(logger_output.name),

            "[logger_root]",
            "level = INFO",
            "handlers=file"
        ]))
        logging_config.flush()

        process = self._run_async(['--logging-config', logging_config.name])
        # Unfortunately the best way to let the process startup until we get
        # Better eventing in the code.
        time.sleep(1)
        try:
            line1 = "Test1"
            self.input.write(line1 + '\n')
            self.input.flush()
            self.assertTrue(self._output_contains([line1]))

            line2 = "Test2"
            self.input.write(line2 + '\n')
            self.input.flush()
            self.assertTrue(self._output_contains([line1, line2]))

            with open(logger_output.name, 'r') as logfile:
                lines = logfile.readlines()
                self.assertTrue(len(lines) > 0)
                for line in lines:
                    self.assertTrue('TEST' in line)
        finally:
            process.terminate()
