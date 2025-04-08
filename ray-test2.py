
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

    def __init__(self):
        self.comms = None

    @ray.remote
    def squares(self, x, log):
        for y in range(random.randint(5, 10)):
            log.add_to_log.remote(x, f"Log from {x}/{y}")
            time.sleep(0.5)
        return x*x

    def process(self, log):

            print("Starting")
            futures = [self.squares.remote(self, i, log) for i in range(5)]
            while futures:
                ready_refs, futures = ray.wait(futures, num_returns=1, timeout=0.5)

                for x in range(5):
                    resp = ray.get(log.get_log.remote(x))
                    print(resp)

                if ready_refs:
                    res = ray.get(ready_refs[0])
                    if type(res) == int:
                        print(f"   --> ANSWER: {res}")

if __name__ == "__main__":
    ray.init(log_to_driver=False)

    log_actor = LoggingActor.remote()

    p = Process()
    p.process(log_actor)

