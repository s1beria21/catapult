import logging
import multiprocessing as mp
import os
import signal
import time

from redis_consumer import redis_consumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [PID:%(process)d][%(threadName)s] %(levelname)s - %(message)s"
)


def worker(sec: int):
    """
    """
    logging.info('worker started')
    time.sleep(sec)
    logging.info('worker ended')


def consumer_start():
    """
    """
    val = redis_consumer('start')
    return int(val)


def consumer_terminate():
    """
    """
    logging.info('start terminate process')
    while True:
        val = redis_consumer('terminate')
        if val == 'stop':
            break
    logging.info('end terminate process')
    

def catapult():
    """
    """
    logging.info('catapult started')
    process, terminate = None, None
    while True:
        if process is None:
            time_process = consumer_start()
            process = mp.Process(target=worker, kwargs={'sec': int(time_process)}, daemon=True)
            terminate = mp.Process(target=consumer_terminate, daemon=True)
            process.start()
            terminate.start()
        else:
            while True:
                if not terminate.is_alive():
                    logging.info('STOP VIA TERMINATE')
                    terminate.join()
                    os.kill(process.pid, signal.SIGTERM)    
                    process.join()
                    process, terminate = None, None
                    break
                elif not process.is_alive():
                    logging.info('STOP VIA END WORKER')
                    process.join()
                    os.kill(terminate.pid, signal.SIGTERM)
                    terminate.join()  
                    process, terminate = None, None
                    break


if __name__ == '__main__':
    catapult()
