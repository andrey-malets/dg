#!/usr/bin/env python3

import argparse
import contextlib
import logging
import os
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
def disk_snapshot(vmm, vm_, timestamp, size):
    with transactions.transact(
        prepare=(
            f'Creating disk snapshot of {vm_.name}',
            lambda: create_vm_disk_snapshot(vmm, vm_, timestamp, size)
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
    ref_vm = vm.WindowsVM(args.ref_vm, args.ref_host)
    cow.check_preconditions(vmm, ref_vm)

    timestamp = cow.generate_timestamp()
    with contextlib.ExitStack() as snapshot_stack:
        snapshot_stack.enter_context(vm.vm_shut_down(vmm, ref_vm))
        snapshot_disk = snapshot_stack.enter_context(disk_snapshot(
            vmm, ref_vm, timestamp, args.snapshot_size
        ))
        new_disk = snapshot_stack.enter_context(another_disk(
            vmm, ref_vm, snapshot_disk
        ))
        logging.info('New VM disk is: %s', new_disk)
        snapshot_vm = snapshot_stack.enter_context(started(vmm, ref_vm))
        wait.wait_for(lambda: snapshot_vm.is_accessible(), 300, 5)

        sysprep_xml = 'sysprep.xml'
        windows.scp(snapshot_vm.host, args.sysprep_xml, sysprep_xml)

        sysprep_cmd = (
            r'start /w C:\Windows\system32\sysprep\sysprep.exe '
            f'/oobe /generalize /shutdown /unattend:{sysprep_xml}'
        )
        snapshot_stack.enter_context(
            windows.ssh_away(snapshot_vm.host, sysprep_cmd)
        )
        logging.info('Waiting for %s to shut down', snapshot_vm.host)
        wait.wait_for(lambda: not vmm.is_vm_running(snapshot_vm),
                      timeout=600, step=10)


def clean_snapshots(args):
    vmm = vm.Virsh()
    print(vmm)
    # TODO:


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
    add_parser.add_argument('-s', '--snapshot-size', default='5G')
    add_parser.add_argument(
        '-sp', '--sysprep-xml',
        default=os.path.join(os.path.dirname(__file__), 'sysprep.xml')
    )

    add_parser.add_argument('ref_vm')
    add_parser.add_argument('ref_host')
    add_parser.set_defaults(func=add_snapshot)

    clean_parser = subparsers.add_parser('clean', help='Cleanup old snapshots')
    clean_parser.add_argument('--force-old', action='store_true')
    clean_parser.add_argument('--force-latest', action='store_true')

    clean_parser.add_argument('ref_vm')
    # clean_parser.add_argument('output')
    clean_parser.set_defaults(func=clean_snapshots)

    return parser.parse_args(config.get_args(raw_args))


def main(raw_args):
    args = parse_args(raw_args)
    log.configure_logging(args)
    with lock.locked(args):
        return args.func(args)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
