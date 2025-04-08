
import os
import time
import random
import logging
import ray
import sys

@ray.remote
class LoggingActor:
    def __init__(self):
        self.logs = {}
    def add_to_log(self, which, txt):
        try:
            self.logs[which].append(txt)
        except:
            self.logs[which] = [txt]
    def get_log(self, which):
        try:
            l = self.logs[which]
            self.logs[which] = []
            return l
        except:
            self.logs[which] = []
            return []

class Process:

    @ray.remote
    def squares(self, x, log):
        for y in range(random.randint(5, 10)):
            log.add_to_log.remote(x, f"Log from {x}/{y}")
            time.sleep(0.3 + (random.randint(1,6) / 10))
        return x*x

    def process(self, log, max_workers):
            print("Starting")
            futures = [self.squares.remote(self, i, log) for i in range(max_workers)]
            while futures:
                ready_refs, futures = ray.wait(futures, num_returns=1, timeout=0.5)
                for x in range(max_workers):
                    resp = ray.get(log.get_log.remote(x))
                    for r in resp:
                        print(r)
                if ready_refs:
                    res = ray.get(ready_refs[0])
                    if type(res) == int:
                        print(f"   --> ANSWER: {res}")

if __name__ == "__main__":
    ray.init(log_to_driver=False)
    log_actor = LoggingActor.remote()
    max_workers = 10
    p = Process()
    p.process(log_actor, max_workers)
