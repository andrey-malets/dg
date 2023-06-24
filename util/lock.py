import contextlib
import dataclasses
import errno
import fcntl
import logging
import sys


@dataclasses.dataclass(order=True)
class Lock:
    file_name: str
    file_mode: str
    lock_mode: int


def get_mode(mode_string):
    if mode_string == 'r':
        return fcntl.LOCK_SH
    elif mode_string == 'w':
        return fcntl.LOCK_EX
    else:
        raise RuntimeError(f'Unknown lock mode "{mode_string}"')


def lock(spec):
    file_name, mode_string = (
        spec.split(',', 1) if ',' in spec
        else (spec, 'w')
    )
    return Lock(file_name=file_name, file_mode=mode_string,
                lock_mode=get_mode(mode_string))


@contextlib.contextmanager
def locked(state, lock):
    with open(lock.file_name, lock.file_mode) as lock_f:
        try:
            state.log.info('Locking %s', lock.file_name)
            fcntl.lockf(lock_f, lock.lock_mode | fcntl.LOCK_NB)
            yield
        except OSError as e:
            if e.errno == errno.EAGAIN:
                logging.error('%s is already locked, exiting', lock.file_name)
                sys.exit(2)
            raise
        else:
            state.log.info('Unlocking %s', lock.file_name)
            fcntl.lockf(lock_f, fcntl.LOCK_UN)
