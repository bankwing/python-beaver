# -*- coding: utf-8 -*-
import errno
import os
import stat
import time
import logging
import multiprocessing
import datetime

from beaver.utils import eglob
from beaver.base_log import BaseLog
from beaver.worker.tail import Tail


class TailManager(multiprocessing.Process, BaseLog):
    def __init__(self, beaver_config, queue_consumer_function, callback, logger=None, consumer_refresh_interval=5.0):
        super(TailManager, self).__init__()
        self._logger = logger
        if not self._logger:
            self._logger = logging.getLogger('beaver')
        self._beaver_config = beaver_config
        self._folder = self._beaver_config.get('path')
        self._callback = callback
        self._create_queue_consumer = queue_consumer_function
        self._discover_interval = beaver_config.get('discover_interval', 15)
        self._log_template = "[TailManager] - {0}"

        self._number_of_consumer_processes = int(self._beaver_config.get('number_of_consumer_processes'))
        self._queue_consumer_function = queue_consumer_function
        self._consumer_refresh_interval = consumer_refresh_interval

        self._tails = {}
        self._update_time = None

        self._shutdown_requested = multiprocessing.Event()

    def listdir(self):
        """HACK around not having a beaver_config stanza
        TODO: Convert this to a glob"""
        ls = os.listdir(self._folder)
        return [x for x in ls if os.path.splitext(x)[1][1:] == "log"]

    def watch(self, paths=[]):
        for path in paths:
            if self._shutdown_requested.is_set():
                break

            tail = Tail(
                filename=path,
                beaver_config=self._beaver_config,
                callback=self._callback,
                logger=self._logger
            )

            if tail.active:
                self._tails[tail.fid()] = tail

    def run(self, interval=0.1, shutdown_timeout=60.0):
        consumer_manager = ConsumerManager(self._queue_consumer_function,
                                           number_of_consumer_processes=self._number_of_consumer_processes,
                                           interval=self._consumer_refresh_interval,
                                           logger=self._logger)
        consumer_manager.start()

        try:
            while not self._shutdown_requested.is_set():
                for fid in self._tails.keys():

                    self.update_files()

                    self._log_debug("Processing {0}".format(fid))
                    if self._shutdown_requested.is_set():
                        break

                    self._tails[fid].run(once=True)

                    if not self._tails[fid].active:
                        self._tails[fid].close()
                        del self._tails[fid]

                self.update_files()
                self._shutdown_requested.wait(interval)

        finally:
            consumer_manager.stop(shutdown_timeout)

    def update_files(self):
        """Ensures all files are properly loaded.
        Detects new files, file removals, file rotation, and truncation.
        On non-linux platforms, it will also manually reload the file for tailing.
        Note that this hack is necessary because EOF is cached on BSD systems.
        """
        if self._update_time and int(time.time()) - self._update_time < self._discover_interval:
            return

        self._update_time = int(time.time())

        possible_files = []
        files = []
        if len(self._beaver_config.get('globs')) > 0:
            extend_files = files.extend
            for name, exclude in self._beaver_config.get('globs').items():
                globbed = [os.path.realpath(filename) for filename in eglob(name, exclude)]
                extend_files(globbed)
                self._beaver_config.addglob(name, globbed)
                self._callback(("addglob", (name, globbed)))
        else:
            append_files = files.append
            for name in self.listdir():
                append_files(os.path.realpath(os.path.join(self._folder, name)))

        for absname in files:
            try:
                st = os.stat(absname)
            except EnvironmentError, err:
                if err.errno != errno.ENOENT:
                    raise
            else:
                if not stat.S_ISREG(st.st_mode):
                    continue
                elif (int(self._beaver_config.get('ignore_old_files_days')) > 0 or \
                     int(self._beaver_config.get('ignore_old_files_hours')) > 0 or \
                     int(self._beaver_config.get('ignore_old_files_minutes')) > 0 \
                     ) and datetime.datetime.fromtimestamp(st.st_mtime) < (datetime.datetime.today() - datetime.timedelta(days=int(self._beaver_config.get('ignore_old_files_days')), hours=int(self._beaver_config.get('ignore_old_files_hours')), minutes=int(self._beaver_config.get('ignore_old_files_minutes')))): 
                    self._logger.debug('[{0}] - file {1} older then {2} days {3} hours {4} minutes so ignoring it'.format(self.get_file_id(st), absname, self._beaver_config.get('ignore_old_files_days'), self._beaver_config.get('ignore_old_files_hours'), self._beaver_config.get('ignore_old_files_minutes')))
                    continue
                append_possible_files = possible_files.append
                fid = self.get_file_id(st)
                append_possible_files((fid, absname))

        # add new ones
        new_files = [fname for fid, fname in possible_files if fid not in self._tails]
        self.watch(new_files)

    def close(self, signalnum=None, frame=None):
        """Closes all currently open Tail objects"""
        self._log_info("Closing all tail objects")
        self._shutdown_requested.set()
        for fid in self._tails:
            self._tails[fid].close()

    @staticmethod
    def get_file_id(st):
        return "%xg%x" % (st.st_dev, st.st_ino)


class ConsumerManager(multiprocessing.Process, BaseLog):
    def __init__(self, queue_consumer_function, number_of_consumer_processes=1, interval=5.0, logger=None):
        """
        Separate process who's sole responsibility is to monitor and respawn consumer processes.
        We keep this logic in a separate simple process to avoid accidental memory leaks while forking.
        :param queue_consumer_function: The function to call to create a queue consumer.
        :param number_of_consumer_processes: The number of queue consumers to run in parallel
        :param interval: How often to check and refresh consumers that have died.
        :param logger: An optional logger.
        """
        assert queue_consumer_function is not None
        super(ConsumerManager, self).__init__()
        self._start_consumer = queue_consumer_function
        self._number_of_consumer_processes = number_of_consumer_processes
        self._proc = [None] * self._number_of_consumer_processes
        self._interval_seconds = interval
        self.stop_flag = multiprocessing.Event()
        self._logger = logger
        self._log_template = "[ConsumerManager] - {0}"

    def run(self):
        """
        Infinite run loop that checks and refreshes consumers every interval seconds.
        Exits when the stop_flag is set via func:`stop`
        """
        while True:
            self._create_queue_consumer_if_required()
            if self.stop_flag.wait(self._interval_seconds):
                break

        for n in range(0, self._number_of_consumer_processes):
            if self._proc[n] is not None and self._proc[n].is_alive():
                self._log_debug("Terminate Process: {0}".format(n))
                self._proc[n].terminate()
                self._proc[n].join()

    def stop(self, timeout=None):
        """
        Stop the process. Can only be called from parent process, not within this process, or from any other
        process other than the parent process.
        :param timeout: How long to wait for graceful shutdown of this process before force terminating.
        :raises RuntimeError if the process did not shut down cleanly.
        """
        self.stop_flag.set()
        self.join(timeout=timeout)
        if self.is_alive():
            self.terminate()
            raise RuntimeError("ConsumerManager did not exit within timeout of {0} seconds".format(timeout))

    def _create_queue_consumer_if_required(self):
        for n in range(0, self._number_of_consumer_processes):
            if not (self._proc[n] and self._proc[n].is_alive()):
                self._log_debug("creating consumer process: {0}".format(n))
                self._proc[n] = self._start_consumer()
