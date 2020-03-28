import time
from functools import wraps

import app


class retry(object):
    """
    Decorator that will try to call the function a specified
    number of times after exponential delay in seconds.

    :param int retries: Maximum function invocations count. default: -1 (infinite).
    :param int delay: Delay between calls in seconds. default: 0 (no delay).
    :param int max_delay: The maximum seconds of delay. default: 0 (no limit).
    :param tuple retry_on: This tuple specifies which error codes we want to retry on. default: 429.
    """
    def __init__(self, retries: int = -1, delay: int = 0, max_delay: int = 0, retry_on: tuple = (429,)):
        self.Logger = app.get_logger(__name__)
        self.retries = retries
        self.delay = delay
        self.max_delay = max_delay
        self.retry_on = retry_on

        if delay > 0 and 0 < max_delay < delay:
            raise Exception('max_delay must be greater than delay.')

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt: int = 0
            while self.retries < 0 or attempt < self.retries:
                attempt += 1
                output = func(*args, **kwargs)
                results = output.Results
                if output.ActionStatus == app.ActionStatus.ERROR \
                        and results in self.retry_on:
                    exp_delay = self._exponential_delay(attempt, self.delay, self.max_delay)
                    self.Logger.info(f"Retrying after {exp_delay} seconds")
                    time.sleep(exp_delay)
                else:
                    return output

        return wrapper

    def _exponential_delay(self, multiplier: int, delay: int, max_delay: int) -> int:
        exp_delay = delay ^ multiplier
        return exp_delay if not max_delay or exp_delay < max_delay else max_delay
