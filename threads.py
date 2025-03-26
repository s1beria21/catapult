import logging
import multiprocessing as mp
import os
import signal
import threading as thr
import time

from redis_consumer import redis_consumer_check_message


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [PID:%(process)d][%(threadName)s] %(levelname)s - %(message)s"
)

stop_thread = thr.Event()


def worker(sec: int):
    """
    """
    logging.info('worker started')
    time.sleep(sec)
    logging.info('worker ended')


def consumer_start():
    """
    """
    logging.info('start consumer process')
    mq = redis_consumer_check_message('start')
    while True:
        msg = mq.get_message()
        if msg is not None and msg['type'] == 'message':
            return msg['data'].decode()
        time.sleep(1)


class ConsumerTerminate(thr.Thread):
    def __init__(self):
        super().__init__()
        self._stop_event = thr.Event()
        self.stop_flag = False

    def stop(self):
        self._stop_event.set()

    def run(self):
        """
        """
        logging.info('start terminate process')
        mq = redis_consumer_check_message('terminate')
        while not self._stop_event.is_set():
            msg = mq.get_message()
            if msg is not None and msg['type'] == 'message' and msg['data'].decode() == 'stop':
                self.stop_flag = True
                logging.info('end terminate process')
            time.sleep(1)


def catapult():
    """
    """
    process, terminate_thread = None, None
    while True:
        if process is None:
            time_process = consumer_start()
            process = mp.Process(target=worker, kwargs={'sec': int(time_process)}, daemon=True)
            terminate_thread = ConsumerTerminate()
            process.start()
            terminate_thread.start()
        else:
            while True:
                if terminate_thread.stop_flag:
                    logging.info('STOP VIA TERMINATE')
                    os.kill(process.pid, signal.SIGTERM)
                    terminate_thread.stop()
                    terminate_thread.join()
                    process.join()
                    process, terminate_thread = None, None
                    break
                elif not process.is_alive():
                    logging.info('STOP VIA END WORKER')
                    terminate_thread.stop()
                    terminate_thread.join()
                    process.join()
                    process, terminate_thread = None, None
                    break

if __name__ == '__main__':
    catapult()