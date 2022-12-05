import contextlib
import errno
import fcntl
import sys


@contextlib.contextmanager
def locked(state, lock):
    if lock:
        with open(lock, 'w') as lock_file:
            try:
                state.log.info('locking %s', lock)
                fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                yield
            except OSError as e:
                if e.errno == errno.EAGAIN:
                    state.log.error('%s is already locked, exiting', lock)
                    sys.exit(2)
                raise
            else:
                state.log.info('unlocking %s', lock)
                fcntl.lockf(lock_file, fcntl.LOCK_UN)
    else:
        yield
