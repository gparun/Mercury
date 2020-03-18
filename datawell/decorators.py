import time
from datetime import datetime, timedelta
from functools import wraps

import app


class retrying(object):
    """
    Decorator that will try to call the function a specified
    number of times after a given timeout in seconds.

    :param int retries: Maximum function invocations count.
    :param int delay: Delay between calls in seconds.
    """
    def __init__(self, retries: int = 1, delay: int = 0):
        if retries < 1:
            raise Exception('Attempts must be greater than zero.')

        self.Logger = app.get_logger(__name__)
        self.retries = retries
        self.delay = delay

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(self.retries):
                result = func(*args, **kwargs)
                self.last_call = datetime.min
                if isinstance(result, int) and result == 429:
                    self.Logger.info(f"Retrying after {self.delay} seconds")
                    time.sleep(self.delay)
                else:
                    return result
            self.Logger.warn("All attempts failed. Increase retries and delay.")

        return wrapper
