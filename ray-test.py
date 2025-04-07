
import time
import random
import logging
import ray
ray.init(
    logging_config=ray.LoggingConfig(encoding="JSON", log_level="INFO", additional_log_standard_attrs=['name'])
)

logger = logging.getLogger("test")

class Process:

    @ray.remote(num_returns="dynamic")
    def squares(self, x):
        for y in range(random.randint(1, 10)):
            time.sleep(y/10.0)
            yield f"Log Message for {x}/{y}"
        return x*x

    def process(self):
        futures = [self.squares.remote(self, i) for i in range(10)]
        done = 0
        logger = logging.getLogger("test")
        logger.info("Starting")
        while done < 10:
            ready_refs, futures = ray.wait(futures, num_returns=1, timeout=None)
            # ready_refs has one result in it
            res = ray.get(ready_refs[0])
            for i, ref in enumerate(res):
                print(ray.get(ref))
            #print(res)
            done += 1


p = Process()
p.process()


# https://docs.ray.io/en/latest/ray-core/patterns/index.html
