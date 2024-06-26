import argparse
import contextlib
import json
import sys

from . import log
from . import stage
from . import state
from util import amt_creds, proc, lock


def config(value):
    with open(value) as config_input:
        return json.load(config_input)


def get_args(raw_args):
    config_parser = argparse.ArgumentParser(raw_args[0], add_help=False)
    config_parser.add_argument(
        '--config', type=config,
        help='Path to config file with all the options in JSON'
    )
    known_args, unknown_args = config_parser.parse_known_args(raw_args[1:])
    if known_args.config is not None:
        if unknown_args:
            config_parser.error(
                '--config is not compatible with other options'
            )
        return known_args.config
    else:
        return raw_args[1:]


def execute_with(raw_args, methods):
    args = get_args(raw_args)
    method_cls, stages = Option.choose_method_and_stages(methods, args)

    if stages == []:
        print(f'Stages of "{method_cls.name}" method:', file=sys.stderr)
        for index, stage_ in enumerate(method_cls.stages):
            print('{:-3d}: {}'.format(index, stage_), file=sys.stderr)
        return 0

    method = method_cls(stages)
    parser = Option.get_method_parser(method, args)
    method_args = parser.parse_args(args)

    method.parse(method_args)

    the_state = state.State(parser, method_args)
    with contextlib.ExitStack() as stack:
        stack.enter_context(log.capturing(method_args, the_state))
        if method_args.lock:
            for lock_file in sorted(method_args.lock):
                stack.enter_context(lock.locked(the_state, lock_file))
        return 0 if method.run(the_state) else 1


class Option(object):
    requirements = []
    EMPTY = ()

    @staticmethod
    def fix_default(kwargs):
        rv = dict(kwargs)
        if kwargs.get('default') == Option.EMPTY:
            rv['default'] = []
        return rv

    @staticmethod
    def add_common_params(parser, method_classes):
        state.State.add_params(parser)
        log.add_params(parser)
        parser.add_argument(
            '--lock', type=lock.lock, nargs='+',
            help='Lock specified file while running deploy. Use exclusive '
                 'lock by default, shared lock can be specified by appending '
                 '",r" to file name'
        )
        parser.add_argument(
            '-m', choices=[method.name for method in method_classes],
            help='Deploy method', required=True)
        parser.add_argument(
            '-s', nargs='*', default=None, metavar='NUM',
            help='Explicitly choose method stages by index. '
                 'Use empty value to list')

    @staticmethod
    def get_stages(specs):
        if specs is None:
            return
        stages = []
        for spec in specs:
            if '-' in spec:
                left, right = spec.split('-')
                stages.extend(range(int(left), int(right) + 1))
            else:
                stages.append(int(spec))
        return stages

    @staticmethod
    def choose_method_and_stages(method_classes, raw_args):
        names = dict((m.name, m) for m in method_classes)
        description = 'Deploy some machines. Available methods are:\n'
        for method in method_classes:
            description += '  {:<8} {}\n'.format(method.name, method.__doc__)
        parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.RawTextHelpFormatter)
        Option.add_common_params(parser, method_classes)
        Option.add_all(parser)
        args = parser.parse_args(raw_args)
        return names[args.m], Option.get_stages(args.s)

    @staticmethod
    def get_method_parser(method, raw_args):
        parser = argparse.ArgumentParser(description=method.__doc__)
        Option.add_common_params(parser, [type(method)])
        Option.add_required(parser, method)
        return parser

    @staticmethod
    def requires(*args, **kwargs):
        def ret(cls):
            Option.requirements.append((cls, args, kwargs))
            return cls
        return ret

    @staticmethod
    def add_all(parser):
        for _, args, kwargs in Option.requirements:
            parser.add_argument(*args, **Option.fix_default(kwargs))

    @staticmethod
    def add_required(parser, method):
        required = set()
        for stage_ in method.stages:
            for cls, args, kwargs in Option.requirements:
                if isinstance(stage_, cls):
                    required.add((args, frozenset(kwargs.items())))
        for args, skwargs in required:
            kwargs = dict(skwargs)
            required = 'default' not in kwargs
            parser.add_argument(
                required=required, *args, **Option.fix_default(kwargs))


@Option.requires('-a', help='amtredird url', metavar='AMTREDIRD',
                 default='https://urgu.org/amtredird')
class WithAMTRedirdURL(stage.Stage):
    def parse(self, args):
        super(WithAMTRedirdURL, self).parse(args)
        self.amtredird_url = args.a


@Option.requires('-c', help='config API url', metavar='CONFIG',
                 default='https://urgu.org/config')
class WithConfigURL(stage.Stage):
    def parse(self, args):
        super(WithConfigURL, self).parse(args)
        self.config_url = args.c


@Option.requires('-p', help='AMT credentials', metavar='FILE',
                 default='amtpasswd')
class WithAMTCredentials(stage.Stage):
    def parse(self, args):
        super(WithAMTCredentials, self).parse(args)
        self.amt_creds = amt_creds.AMTCredentialsProvider(args.p)


@Option.requires('-ll', help='ssh login for Linux',
                 metavar='LOGIN', default='root')
@Option.requires('-lw', help='ssh login for Windows',
                 metavar='LOGIN', default='Administrator')
class WithSSHCredentials(stage.Stage):
    def get_login(self):
        return self.ssh_login_linux

    def parse(self, args):
        super(WithSSHCredentials, self).parse(args)
        self.ssh_login_linux = args.ll
        self.ssh_login_windows = args.lw

    def run_scp(self, host, login, src, dst):
        return proc.run_process(
            ['scp', '-o', 'PasswordAuthentication=no',
             src, '{}@{}:{}'.format(login, host, dst)],
            host.state.log)

    def run_ssh(self, host, args, login, opts=None):
        return proc.run_remote_process(
            host.name, login, args, host.state.log, opts)

    def run_ssh_checked(self, host, args, login, description, opts=None):
        rv, output = self.run_ssh(host, args, login, opts=opts)
        if rv:
            raise RuntimeError(f'failed to {description}')
        return output


@Option.requires('-l', help='Local address', metavar='ADDR')
class WithLocalAddress(stage.Stage):
    def parse(self, args):
        super(WithLocalAddress, self).parse(args)
        self.local_addr = args.l


@Option.requires('-nc', help='Parallel network connections allowed',
                 metavar='CONNECTIONS', type=int, default=2)
@Option.requires('-ns', help='Network speed required on each host, in MB/s',
                 metavar='SPEED', type=int, default=300)
class WithNetworkParallelism(stage.Stage):
    def parse(self, args):
        super(WithNetworkParallelism, self).parse(args)
        self.network_connections = args.nc
        self.network_speed = args.ns


@Option.requires(
    '-n', help='Deploy local INPUT into OUTPUT on all the hosts with ndd',
    metavar='{HOST:}?INPUT{,iargs}?:OUTPUT{,oargs}?{+args}?', action='append',
    default=Option.EMPTY)
@Option.requires(
    '-np', help='ndd port to use for transfers', metavar='PORT',
    default='3634')
class WithNDDArgs(stage.Stage):
    class NDDSpec(object):
        def __init__(self, spec):
            if '+' in spec:
                io, self.args = spec.split('+', 1)
            else:
                io = spec
                self.args = None

            if io.count(':') == 2:
                self.source, ispec, ospec = io.split(':', 2)
            else:
                self.source = None
                ispec, ospec = io.split(':', 1)

            iparts = ispec.split(',')
            self.input_, self.iargs = iparts[0], iparts[1:]

            oparts = ospec.split(',')
            self.output, self.oargs = oparts[0], oparts[1:]

    def parse(self, args):
        super(WithNDDArgs, self).parse(args)
        self.ndds = map(WithNDDArgs.NDDSpec, args.n)
        self.ndd_port = args.np


@Option.requires(
    '-b', help='Ban HOST, excluding it from deployment',
    metavar='HOST', action='append', default=Option.EMPTY)
class WithBannedHosts(stage.Stage):
    def parse(self, args):
        super(WithBannedHosts, self).parse(args)
        self.banned_hosts = args.b


@Option.requires(
    '-wp', help='Windows root partition label',
    metavar='LABEL', default='windows10')
class WithWindowsRootPartition(stage.Stage):
    def parse(self, args):
        super(WithWindowsRootPartition, self).parse(args)
        self.win_root_partition = f'/dev/disk/by-partlabel/{args.wp}'


@Option.requires(
    '-wd', help='Set Windows partition volume path by FS label',
    metavar='LABEL:LETTER', default=None)
class WithWindowsDataPartition(stage.Stage):
    def parse(self, args):
        super(WithWindowsDataPartition, self).parse(args)
        self.win_data_label, self.win_data_letter = (
            args.wd.split(':', 1) if args.wd is not None else [None, None])

    def get_win_data_partition(self):
        return '/dev/disk/by-partlabel/{}'.format(self.win_data_label)


@Option.requires(
    '-d', help='Set windows driver search path',
    metavar='PATH', default=None)
class WithWindowsDriverSearchPath(stage.Stage):
    def parse(self, args):
        super(WithWindowsDriverSearchPath, self).parse(args)
        self.driver_path = args.d
