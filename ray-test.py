
import os
import time
import random
import logging
import ray
import sys
from ray._private import log_monitor

class NewLogMonitor:

    def __init__(self, max_workers, log_dir="."):
        self.log_dir: str = log_dir
        self.max_workers = max_workers
        self.log_files = {}

    def update_log_files(self):
        for fn in os.listdir(self.log_dir):
            if fn.startswith('log_ray_'):
                n = int(fn[8:-4])
                if not n in self.log_files:
                    fh = open(os.path.join(self.log_dir, fn))
                    self.log_files[n] = fh

    def clear_log_files(self):
        for fn in os.listdir(self.log_dir):
            if fn.startswith('log_ray_'):
                os.remove(os.path.join(self.log_dir, fn))

    def start(self):
        # Clear any previous log files
        self.clear_log_files()

    def shutdown(self):
        for fh in self.log_files.values():
            fh.close()
        self.clear_log_files()

    def run_once(self):
        # Read any new lines from files and emit
        if len(self.log_files) < self.max_workers:
            self.update_log_files()
        for n, lfh in self.log_files.items():
            # read from filehandle; fh will maintain seek
            for l in lfh:
                print(f"[{n}]: {l}")

class DriverLogHandler(logging.Handler):
    def __init__(self, prefix, n=-1):
        super().__init__()
        self.n = n
        self.prefix = prefix
        self.filehandle = None

    def emit(self, record):
        # Not sure how to close the filehandle
        if self.n > -1:
            # Write to file
            if self.filehandle is None:
                self.filehandle = open(f"log_ray_{self.n}.txt", "a")
            self.filehandle.write(f"{self.prefix} {record.getMessage()}")
            self.filehandle.flush()
        else:
            # I'm the driver
            print(f" {self.prefix} {record.getMessage()}")

def init_logger():
    l = logging.getLogger("ray")
    l.propagate = False
    l.handlers.clear()
    return l

ray.init(log_to_driver=False)

class Process:
    @ray.remote
    def squares(self, x):
        logger = init_logger()
        logger.addHandler(DriverLogHandler("|||", x))

        for y in range(random.randint(5, 10)):
            logger.warning(f"Log Message for {x}/{y}")
            time.sleep(0.5)
        return x*x

    def process(self):
        logger = init_logger()
        logger.addHandler(DriverLogHandler("$$$"))

        monitor = NewLogMonitor(5)
        monitor.start()

        logger.info("Starting")
        futures = [self.squares.remote(self, i) for i in range(5)]
        while futures:
            ready_refs, futures = ray.wait(futures, num_returns=1, timeout=0.5)
            monitor.run_once()
            if ready_refs:
                res = ray.get(ready_refs[0])
                if type(res) == int:
                    logger.info(f"   --> ANSWER: {res}")
        monitor.run_once()
        monitor.shutdown()
p = Process()
p.process()

