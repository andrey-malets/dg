#!/usr/bin/env python3

import argparse
import contextlib
import dataclasses
import glob
import logging
import os
import subprocess
import sys

from dg.prepare.util import config
from dg.prepare.util import cow
from dg.prepare.util import lock
from dg.prepare.util import log
from dg.prepare.util import lvm
from dg.prepare.util import transactions
from dg.prepare.util import vm
from dg.prepare.util import wait
from dg.prepare.util import windows


SETUP_SCRIPTS_DIR = r'C:\Windows\Setup\Scripts'


DEFAULT_SYSPREP_XML = os.path.join(
    os.path.dirname(__file__), 'util', 'windows', 'sysprep.xml'
)


def win_path(cygwin, path):
    if not cygwin:
        return path
    else:
        drive, source_path = path.split(':', 1)
        cygwin_path = source_path.replace('\\', '/')
        return f'/cygdrive/{drive.lower()}{cygwin_path}'


def escape(cygwin, cmd):
    return cmd.replace('\\', r'\\') if cygwin else cmd


@dataclasses.dataclass
class Copy:
    src: str
    dst: str

    def __init__(self, spec):
        self.src, self.dst = spec.split(':', 1)


def create_vm_disk_snapshot(vmm, vm_, timestamp, size):
    origin = None
    name = None
    lv = vmm.get_disk(vm_)
    wait.wait_for(lambda: not lvm.is_lv_open(lv), timeout=30, step=1)
    origin = lv
    name = lvm.lvm_snapshot_name(origin, timestamp)
    lvm.create_lvm_snapshot(origin, name, non_volatile_pv=None, size=size)

    return os.path.join(os.path.dirname(origin), name)


@contextlib.contextmanager
def disk_snapshot(vmm, vm_, timestamp, size, clean):
    with transactions.transact(
        prepare=(
            f'Creating disk snapshot of {vm_.name}',
            lambda: create_vm_disk_snapshot(vmm, vm_, timestamp, size)
        ),
        commit=(
            'cleaning up disk snapshot' if clean else None,
            lambda result: lvm.remove_lv(result[0]) if clean else None
        ),
        rollback=(
            'cleaning up disk snapshot',
            lambda result: lvm.remove_lv(result[0])
        )
    ) as snapshot_name:
        yield snapshot_name


@contextlib.contextmanager
def another_disk(vmm, vm_, disk):
    old_disk = vmm.get_disk(vm_)
    with transactions.transact(
        prepare=(
            f'Setting disk for {vm_.name} to {disk}',
            lambda: vmm.set_disk(vm_, old_disk, disk)
        ),
        final=(
            f'Setting disk for {vm_.name} back to {old_disk}',
            lambda _: vmm.set_disk(vm_, disk, old_disk)
        )
    ) as new_disk:
        yield new_disk


@contextlib.contextmanager
def started(vmm, vm_):
    with transactions.transact(
        prepare=(f'Starting {vm_.name}', lambda: vmm.start(vm_)),
        rollback=(f'Destroying {vm_.name}', lambda _: vmm.destroy(vm_))
    ):
        yield vm_


def add_snapshot(args):
    vmm = vm.Virsh()
    ref_vm = vm.WindowsVM(args.ref_vm, host=args.ref_host,
                          system_disk=args.system_disk)
    cow.check_preconditions(vmm, ref_vm)

    timestamp = cow.generate_timestamp()
    with contextlib.ExitStack() as snapshot_stack:
        snapshot_stack.enter_context(vm.vm_shut_down(vmm, ref_vm))
        snapshot_disk = snapshot_stack.enter_context(disk_snapshot(
            vmm, ref_vm, timestamp, args.snapshot_size, clean=args.test
        ))
        new_disk = snapshot_stack.enter_context(another_disk(
            vmm, ref_vm, snapshot_disk
        ))
        logging.info('New VM disk is: %s', new_disk)
        snapshot_vm = snapshot_stack.enter_context(started(vmm, ref_vm))
        wait.wait_for(lambda: snapshot_vm.is_accessible(), 300, 5)

        sysprep_xml = rf'C:\Users\{windows.LOGIN}\sysprep.xml'
        windows.scp(snapshot_vm.host, args.sysprep_xml,
                    win_path(args.cygwin, sysprep_xml))

        setup_scripts_dir = win_path(args.cygwin, SETUP_SCRIPTS_DIR)
        windows.ssh(snapshot_vm.host,
                    escape(args.cygwin, f'mkdir {setup_scripts_dir}'),
                    method=subprocess.call)

        for copy in args.copy:
            windows.scp(snapshot_vm.host, copy.src,
                        win_path(args.cygwin, copy.dst))

        cmd_prefix = 'cmd /c ' if args.cygwin else ''

        sysprep_cmd = escape(
            args.cygwin,
            rf'{cmd_prefix}start /w C:\Windows\system32\sysprep\sysprep.exe '
            f'/oobe /generalize /shutdown /unattend:{sysprep_xml}'
        )
        with windows.ssh_away(snapshot_vm.host, sysprep_cmd):
            vmm.wait_to_shutdown(snapshot_vm, timeout=600, step=10)

        if args.link_snapshot:
            logging.info('Linking snapshot %s to %s', new_disk,
                         args.link_snapshot)
            lvm.move_link(new_disk, args.link_snapshot)
        elif args.test:
            logging.info('Starting sysprepped vm back for test')
            snapshot_stack.enter_context(started(vmm, snapshot_vm))
            logging.info('Waiting for %s to become accessible',
                         snapshot_vm.name)
            wait.wait_for(lambda: snapshot_vm.is_accessible(), 900, 5)
            snapshot_vm.shutdown()
            vmm.wait_to_shutdown(snapshot_vm, timeout=60)

    if args.packages:
        with open(args.packages, 'w') as packages_output:
            packages_output.write(
                windows.collect_installed_packages(ref_vm.host, args.cygwin)
            )


def snapshot_glob(origin):
    return f'{origin}-at-*'


def get_snapshots(vmm, vm):
    pattern = snapshot_glob(vmm.get_disk(vm))
    return sorted(glob.glob(pattern))


def clean_snapshots(args):
    vmm = vm.Virsh()
    ref_vm = vm.WindowsVM(args.ref_vm, host=None,
                          system_disk=args.system_disk)
    snapshots = get_snapshots(vmm, ref_vm)
    if not snapshots:
        return

    old, latest = snapshots[:-1], snapshots[-1]

    for snapshot in old:
        logging.info('Removing snapshot %s', snapshot)
        lvm.remove_lv(snapshot)

    if args.force_latest:
        logging.warning('Removing latest snapshot %s', latest)
        lvm.remove_lv(latest)


def parse_args(raw_args):
    parser = argparse.ArgumentParser(raw_args[0])
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-l', '--lock',
                        help='Lock specified file exclusively while '
                             'running an update')
    subparsers = parser.add_subparsers(
        metavar='subcommand', help='subcommand to execute', required=True
    )

    add_parser = subparsers.add_parser('add', help='Add new snapshot')
    add_parser.add_argument('-s', '--snapshot-size', default='20G')
    add_parser.add_argument('-c', '--copy', default=[], action='append',
                            type=Copy)
    add_parser.add_argument('--link-snapshot')
    add_parser.add_argument('-cw', '--cygwin', action='store_true')
    add_parser.add_argument('-t', '--test', action='store_true')
    add_parser.add_argument('-sp', '--sysprep-xml',
                            default=DEFAULT_SYSPREP_XML)
    add_parser.add_argument('--packages',
                            help='Put a list of packages installed on '
                                 'resulting image to specified file')

    add_parser.add_argument('ref_vm')
    add_parser.add_argument('ref_host')
    add_parser.add_argument('--system-disk')
    add_parser.set_defaults(func=add_snapshot)

    clean_parser = subparsers.add_parser('clean', help='Cleanup old snapshots')
    clean_parser.add_argument('--force-latest', action='store_true')

    clean_parser.add_argument('ref_vm')
    clean_parser.add_argument('--system-disk')
    clean_parser.set_defaults(func=clean_snapshots)

    args = parser.parse_args(config.get_args(raw_args))
    if args.func == add_snapshot and args.test and args.link_snapshot:
        parser.error('--test is incompatible with --link-snapshot')

    return args


def main(raw_args):
    args = parse_args(raw_args)
    log.configure_logging(args)
    with lock.locked(args):
        return args.func(args)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
