import contextlib
import random
import time

from tornado import gen


_MAX_INTERVAL = 10
MAX_BACKOFF_WAIT_TIME = 2 ** _MAX_INTERVAL


def generate_backoff(interval, max_wait=MAX_BACKOFF_WAIT_TIME):
    # slight optimization to make sure we're not going crazy
    if interval > _MAX_INTERVAL:
        interval = _MAX_INTERVAL
    return min((2 ** interval) * random.random(), max_wait)


@contextlib.contextmanager
def gen_retry(ioloop, max_retries=100):

    @gen.coroutine
    def waiter():
        wait_time = time.time() + generate_backoff(waiter.increment)
        yield gen.Task(ioloop.add_timeout, wait_time)
        waiter.increment += 1
        if waiter.increment > max_retries:
            raise MaxRetriesExceeded(
                "Attempted operation {0} times.".format(waiter.increment))

    waiter.increment = 0
    yield waiter


class MaxRetriesExceeded(Exception):
    pass
