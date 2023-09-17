import contextlib
import logging


class HostLoggerAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['host'], msg), kwargs


class State(object):
    @staticmethod
    def add_params(parser):
        parser.add_argument(
            '-H', metavar='HOST', help='Host(s) to deploy',
            default=[], action='append')
        parser.add_argument(
            '-g', metavar='GROUP', help='Group(s) to deploy',
            default=[], action='append')

    def __init__(self, parser, args):
        self.hosts, self.groups = args.H, args.g
        if len(self.hosts) == 0 and len(self.groups) == 0:
            parser.error('at least one host or group should be specified')

        self.active_hosts = set()
        self.failed_hosts = set()
        self.all_failed_hosts = set()

        self._current_host = None
        self.logger = logging.getLogger(__name__)

    @contextlib.contextmanager
    def current_host(self, host):
        assert self._current_host is None
        self._current_host = host
        old_logger = self.logger
        try:
            self.logger = HostLoggerAdapter(
                old_logger,
                {'host': self._current_host}
            )
            yield self._current_host
        finally:
            self._current_host = None
            self.logger = old_logger

    @property
    def log(self):
        return self.logger
