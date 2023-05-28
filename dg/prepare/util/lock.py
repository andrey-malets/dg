import contextlib
import errno
import fcntl
import logging
import sys


@contextlib.contextmanager
def locked(args):
    if args.lock:
        with open(args.lock, 'w') as lock:
            try:
                logging.debug('Locking %s', args.lock)
                fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                yield
            except OSError as e:
                if e.errno == errno.EAGAIN:
                    logging.error('%s is already locked, exiting', args.lock)
                    sys.exit(2)
                raise
            else:
                logging.debug('Unlocking %s', args.lock)
                fcntl.lockf(lock, fcntl.LOCK_UN)
    else:
        yield
