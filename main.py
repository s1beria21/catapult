import logging
import multiprocessing as mp
import os
import signal
import time


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
    r = input('waiting (time_process, time_terminate): \n')
    return r


def consumer_terminate(sec: int):
    """
    """
    logging.info('start terminate process')
    time.sleep(sec)
    logging.info('end terminate process')
    

def catapult():
    """
    """
    logging.info('catapult started')
    process, terminate = None, None
    while True:
        if process is None:
            data_from_consumer = consumer_start()
            time_process, time_terminate = data_from_consumer.split(',')
            process = mp.Process(target=worker, kwargs={'sec': int(time_process)}, daemon=True)
            terminate = mp.Process(target=consumer_terminate, kwargs={'sec': int(time_terminate)}, daemon=True)
            process.start()
            terminate.start()
        else:
            while True:
                if not terminate.is_alive():
                    logging.info('Starting terminate...')
                    terminate.join()
                    os.kill(process.pid, signal.SIGTERM)    
                    process.join()
                    process, terminate = None, None
                    break
                elif not process.is_alive():
                    logging.info('Process ending...')
                    process.join()
                    os.kill(terminate.pid, signal.SIGTERM)
                    terminate.join()  
                    process, terminate = None, None
                    break


if __name__ == '__main__':
    catapult()
