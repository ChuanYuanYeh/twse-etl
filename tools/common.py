"""
Common utilities used throughout the project
"""
import time

from config.config import config


def keep_retries(func):
    def operation(*args, **kwargs):
        attempts = 1
        while True:
            try:
                return func(*args, **kwargs)
            except:
                attempts += 1
                time.sleep(0.1)
                if attempts > config['RETRY_LIMIT']:
                    raise
    return operation
