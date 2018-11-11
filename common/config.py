import argparse
import sys

import log, stage, state
from util import amt_creds, proc


def execute_with(raw_args, methods):
    method_cls, stages = Option.choose_method_and_stages(methods, raw_args)

    if stages == []:
        print >> sys.stderr, 'Stages of "{}" method:'.format(
            method_cls.name)
        for index, stage_ in enumerate(method_cls.stages):
            print >> sys.stderr, '{:-3d}: {}'.format(index, stage_)
        return 0

    method = method_cls(stages)
    parser = Option.get_method_parser(method, raw_args)
    method_args = parser.parse_args(raw_args)

    method.parse(method_args)
    the_state = state.State(parser, method_args)

    with log.capturing(method_args, the_state):
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


@Option.requires('-l', help='Local address', metavar='ADDR')
class WithLocalAddress(stage.Stage):
    def parse(self, args):
        super(WithLocalAddress, self).parse(args)
        self.local_addr = args.l


@Option.requires(
    '-n', help='Deploy local INPUT into OUTPUT on all the hosts with ndd',
    metavar='{HOST:}?INPUT:OUTPUT{+args}?', action='append',
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
                self.source, self.input_, self.output = io.split(':', 2)
            else:
                self.source = None
                self.input_, self.output = io.split(':', 1)

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
    '-wp', help='Windows 7 root partition label',
    metavar='LABEL', default='windows7')
class WithWindows7Partition(stage.Stage):
    def parse(self, args):
        super(WithWindows7Partition, self).parse(args)
        self.win7_partition = args.wp

    def get_win7_partition(self):
        return '/dev/disk/by-partlabel/{}'.format(self.win7_partition)


@Option.requires(
    '-wd', help='Set windows partition volume path by FS label',
    metavar='LABEL:LETTER', default=None)
class WithWindowsDataPartition(stage.Stage):
    def parse(self, args):
        self.win_data_label, self.win_data_letter = (
            args.wd.split(':', 1) if args.wd is not None else [None, None])

    def get_win7_data_partition(self):
        return '/dev/disk/by-partlabel/{}'.format(self.win_data_label)


@Option.requires(
    '-d', help='Set windows driver search path',
    metavar='PATH', default=None)
class WithWindowsDriverSearchPath(stage.Stage):
    def parse(self, args):
        super(WithLocalAddress, self).parse(args)
        self.driver_path = args.d
