import glob
import logging
import os
import sys
from multiprocessing import Process

from config import config
from mrds import MyRedis
from worker import WcWorker

logging.basicConfig(level=logging.DEBUG, filename=config["LOGFILE"],
                    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')

def add_files_to_redis(rds: MyRedis):
    files = glob.glob(config["DATA_PATH"])
    logging.info(f"Found {len(files)} files to add to Redis stream")
    for file in files:
        rds.add_file(file)
        logging.info(f"Added file {file} to Redis stream")

def start_worker(worker_type):
    rds = MyRedis()
    worker = WcWorker(worker_type)
    worker.run(rds=rds)

def main():
    rds = MyRedis()
    add_files_to_redis(rds)

    workers = []
    for _ in range(config["N_NORMAL_WORKERS"]):
        p = Process(target=start_worker, args=('normal',))
        workers.append(p)
        p.start()

    for _ in range(config["N_CRASHING_WORKERS"]):
        p = Process(target=start_worker, args=('crashing',))
        workers.append(p)
        p.start()

    for _ in range(config["N_SLEEPING_WORKERS"]):
        p = Process(target=start_worker, args=('sleeping',))
        workers.append(p)
        p.start()

    for worker in workers:
        worker.join()

if __name__ == "__main__":
    main()