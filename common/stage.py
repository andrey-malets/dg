import contextlib
import multiprocessing


class Stage(object):
    def parse(self, args):
        pass

    def __str__(self):
        return self.__class__.__doc__

    def run(self, state):
        raise NotImplementedError

    def rollback(self, state):
        pass


class SimpleStage(Stage):
    def run(self, state):
        for host in sorted(state.active_hosts):
            try:
                self.run_single(host)
            except Exception as e:
                host.fail(self, e)

    def rollback(self, state):
        for host in sorted(state.failed_hosts):
            try:
                self.rollback_single(host)
            except Exception as e:
                state.log.exception('rollback of {} for {} failed: {}'.format(
                    self, host.name, e))

    def run_single(self, host):
        raise NotImplementedError

    def rollback_single(self, host):
        pass


def _run_forked(args):
    stage, host = args
    try:
        with host.state.current_host(host):
            stage.run_single(host)
            return stage.failed, stage.failure_reason
    except Exception as e:
        host.state.log.exception('Parallel stage failed for {}'.format(host))
        return True, 'exception occured: {}'.format(e)


class ParallelStage(Stage):
    HUGE_TIMEOUT = 60 * 60 * 24

    def __init__(self, poolsize=0):
        self.poolsize = poolsize
        self.failed = False
        self.failure_reason = None

    def run(self, state):
        try:
            pool = multiprocessing.Pool(
                self.poolsize if self.poolsize else len(state.active_hosts))

            with self.prepared():
                host_to_result = [
                    (host, pool.apply_async(_run_forked, [(self, host)]))
                    for host in sorted(state.active_hosts)
                ]
                for host, result in host_to_result:
                    # Timeout is here to handle interruptions properly,
                    # otherwise it will not work as expected due to bug.
                    failed, reason = result.get(ParallelStage.HUGE_TIMEOUT)
                    if failed:
                        assert reason
                        host.fail(self, reason)
        except BaseException:
            pool.terminate()
            raise
        finally:
            pool.close()

    def run_single(self, host):
        return False, 'Not implemented.'

    @contextlib.contextmanager
    def prepared(self):
        yield

    def fail(self, reason):
        self.failed = True
        self.failure_reason = reason
