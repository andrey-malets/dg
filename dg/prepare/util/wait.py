import time


class Timeout(Exception):
    pass


def wait_for(condition, timeout, step):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition():
            return True
        time.sleep(step)

    raise Timeout(f'Failed to wait {timeout} seconds for {condition}')
