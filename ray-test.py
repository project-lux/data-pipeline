
import time
import random
import logging
import ray
ray.init(
    logging_config=ray.LoggingConfig(encoding="JSON", log_level="INFO", additional_log_standard_attrs=['name'])
)


def init_logger():
    return logging.getLogger("foo")

logger = init_logger()

class Process:

    @ray.remote
    def squares(self, x):
        logger = init_logger()
        for y in range(random.randint(5, 10)):
            logger.warning(f"Log Message for {x}/{y}")
            time.sleep(2)
        return x*x

    def process(self):
        futures = [self.squares.remote(self, i) for i in range(10)]
        done = 0
        logger = logging.getLogger("test")
        logger.info("Starting")
        while futures:
            ready_refs, futures = ray.wait(futures, num_returns=1, timeout=None)
            # ready_refs has one result in it
            res = ray.get(ready_refs[0])
            if type(res) == int:
                print(f"   --> ANSWER: {res}")
            else:
                for ref in res:
                    print(ray.get(ref))

p = Process()
p.process()


# https://docs.ray.io/en/latest/ray-core/patterns/index.html
