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


def log_execution_time(logger=None, category="", to_log_arguments=False):
    def decorator(func):
        function_module = func.__module__
        _logger = app.get_logger(function_module) if logger is None else logger

        @wraps(func)
        def log_execution_time_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            execution_time = int(round((time.perf_counter() - start_time) * 1000))

            function_name = func.__name__
            message_info = {
                "Type": "execution_time",
                "Execution_time": execution_time,
                "Function": {"Name": function_name, "Module": function_module}
            }
            if category != "":
                message_info["Category"] = category
            if to_log_arguments:
                args_str = ', '.join(repr(arg) for arg in args)
                message_info["Function"].update({"args": args_str, "kwargs": kwargs})

            _logger.info(f"{function_name}: Execution_time: {execution_time} ms, Category: {category}",
                         extra={"message_info": message_info})
            return result
        return log_execution_time_wrapper
    return decorator


def publish_running_time_metric(namespace, module):
    def decorator(func):
        import boto3
        cloud_watch_client = boto3.client('cloudwatch')

        @wraps(func)
        def execution_time_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            running_time = int(round((time.perf_counter() - start_time) * 1000))
            cloud_watch_client.put_metric_data(
                MetricData=[
                    {
                        'MetricName': 'Running time',
                        'Dimensions': [
                            {
                                'Name': 'Module Name',
                                'Value': module
                            }
                        ],
                        'Unit': 'Milliseconds',
                        'Value': running_time
                    }
                ],
                Namespace=namespace
            )
            return result
        return execution_time_wrapper
    return decorator
