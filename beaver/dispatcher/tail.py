# -*- coding: utf-8 -*-
import multiprocessing
import Queue
import signal
import time

from beaver.config import BeaverConfig
from beaver.run_queue import run_queue
from beaver.ssh_tunnel import create_ssh_tunnel
from beaver.utils import REOPEN_FILES, setup_custom_logger
from beaver.worker.tail_manager import TailManager


def run(args=None):

    logger = setup_custom_logger('beaver', args)
    beaver_config = BeaverConfig(args, logger=logger)

    if beaver_config.get('logstash_version') not in [0, 1]:
        raise LookupError("Invalid logstash_version")

    queue = multiprocessing.JoinableQueue(beaver_config.get('max_queue_size'))

    manager_proc = None
    termination_requested = multiprocessing.Event()
    ssh_tunnel = create_ssh_tunnel(beaver_config, logger=logger)

    def queue_put(*args):
        return queue.put(*args)

    def queue_put_nowait(*args):
        return queue.put_nowait(*args)

    def request_shutdown(signalnum, frame):
        termination_requested.set()
        if signalnum is not None:
            sig_name = tuple((v) for v, k in signal.__dict__.iteritems() if k == signalnum)[0]
            logger.info("{0} detected".format(sig_name))
            logger.info("Shutting down. Please wait...")
        else:
            logger.info('Worker process cleanup in progress...')

    def cleanup():
        try:
            queue_put_nowait(("exit", ()))
        except Queue.Full:
            pass

        if manager_proc is not None:
            try:
                manager_proc.close()
                manager_proc.join()
            except RuntimeError:
                pass

        if ssh_tunnel is not None:
            logger.info("Closing ssh tunnel...")
            ssh_tunnel.close()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGQUIT, request_shutdown)

    def create_queue_consumer():
        process_args = (queue, beaver_config, logger)
        proc = multiprocessing.Process(target=run_queue, args=process_args)

        logger.info("Starting queue consumer")
        proc.start()
        return proc

    def create_queue_producer():
        return TailManager(
            beaver_config=beaver_config,
            queue_consumer_function=create_queue_consumer,
            callback=queue_put,
            logger=logger
        )

    last_start = None
    while not termination_requested.is_set():

        try:

            if REOPEN_FILES:
                logger.debug("Detected non-linux platform. Files will be reopened for tailing")

            if manager_proc is None or not manager_proc.is_alive():
                logger.info('Starting worker...')
                manager_proc = create_queue_producer()
                manager_proc.start()
                last_start = time.time()
                logger.info('Working...')

            if beaver_config.get('refresh_worker_process') and manager_proc.is_alive():
                if last_start and beaver_config.get('refresh_worker_process') < time.time() - last_start:
                    logger.info('Worker has exceeded refresh limit. Terminating process...')
                    cleanup()
            else:
                # Workaround for fact that multiprocessing.Event.wait() deadlocks on main thread
                # And blocks SIGINT signals from getting through.
                while not termination_requested.is_set():
                    time.sleep(0.5)

        except KeyboardInterrupt:
            pass
    cleanup()

