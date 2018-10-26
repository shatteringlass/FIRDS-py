from funcutils import wraps
from requests import ConnectionError
from socket import gaierror
from time import sleep


def retry(func):
    """Catches connection errors, waits and retries"""
    @wraps(func)
    def retry_wrapper(*args, **kwargs):
        self = args[0]
        error = None
        for _ in range(self.retry_count):
            try:
                result = func(*args, **kwargs)
            except (ConnectionError, gaierror) as e:
                error = e
                print("Connection Error, retrying in {} seconds".format(
                    self.retry_delay))
                sleep(self.retry_delay)
                continue
            else:
                return result
        else:
            raise error
    return retry_wrapper
