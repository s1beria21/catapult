"""
Катапульта на процессах.

Проблема катапульты на тредах в том, что чтобы дропнуть треду, которая
следит за terminate статусом, нужно передать ей эвент. Передать можно, но
на стороне треды нужно писать обработчик. А если операция получения данных
о терминации блокирующая, то до обработчика мы не дойдем.
"""

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


def consumer_terminate(pipe_conn):
    """
    """
    logging.info('start terminate process')
    while True:
        val = redis_consumer('terminate')
        if val == 'stop':
            pipe_conn.send("stop")
    logging.info('end terminate process')
    

def catapult():
    """
    """
    logging.info('catapult started')
    process, terminate, pipe_from_main = None, None, None
    while True:
        if process is None:
            time_process = consumer_start()
            process = mp.Process(target=worker, kwargs={'sec': int(time_process)}, daemon=True)
            
            pipe_from_main, pipe_to_terminate = mp.Pipe()

            terminate = mp.Process(target=consumer_terminate, args=(pipe_to_terminate,), daemon=True)
            process.start()
            terminate.start()
        else:
            while True:
                if pipe_from_main is not None and pipe_from_main.poll(1):
                    logging.info('STOP VIA TERMINATE')
                    os.kill(process.pid, signal.SIGTERM)  

                    os.kill(terminate.pid, signal.SIGTERM)    
                    terminate.join()
                    process.join()
                    process, terminate, pipe_from_main = None, None, None
                    break
                elif not process.is_alive():
                    logging.info('STOP VIA END WORKER')
                    os.kill(terminate.pid, signal.SIGTERM)
                    terminate.join()
                    process.join()
                    process, terminate, pipe_from_main = None, None, None
                    break


if __name__ == '__main__':
    catapult()
