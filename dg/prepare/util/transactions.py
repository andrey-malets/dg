import contextlib
import logging


@contextlib.contextmanager
def transact(prepare=None, final=None, commit=None, rollback=None):
    assert final is None or (commit is None and rollback is None), (
        'final action must only be present with no commit and rollback'
    )
    rv = None
    if prepare is not None:
        prepare_msg, prepare_fn = prepare
        if prepare_msg is not None:
            logging.info(prepare_msg)
        rv = prepare_fn()
    try:
        yield rv
    except BaseException as e:
        if any((final, rollback)):
            rollback_msg, rollback_fn = next(filter(None, (final, rollback)))
            if rollback_msg:
                logging.warning(rollback_msg)
            try:
                rollback_fn((rv, e))
            except Exception:
                logging.exception('Exception while %s', rollback_msg)
        raise
    else:
        if any((final, commit)):
            commit_msg, commit_fn = next(filter(None, (final, commit)))
            if commit_msg:
                logging.info(commit_msg)
            try:
                commit_fn((rv, None))
            except Exception:
                logging.exception('Exception while %s', commit_msg)
