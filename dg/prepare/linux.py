#!/usr/bin/env python3

import argparse
import contextlib
import glob
import logging
import os
import re
import shutil
import sys

from dg.prepare.util import config
from dg.prepare.util import cow
from dg.prepare.util import disk
from dg.prepare.util import ipxe
from dg.prepare.util import iscsi
from dg.prepare.util import linux as linux_util
from dg.prepare.util import lock
from dg.prepare.util import log
from dg.prepare.util import lvm
from dg.prepare.util import mount
from dg.prepare.util import processes
from dg.prepare.util import vm
from dg.prepare.util import volume_cache
from dg.prepare.util import wait


def get_hostname(session, host_re=re.compile(r'^.+\:(?P<hostname>.+)'
                                             r'_....-..-.._..-..-..$')):
    match = host_re.match(session)
    if not match:
        raise ValueError(f'Session name {session} did not match any hostname')
    return match.group('hostname')


def booted_properly(vm, timestamp):
    if not vm.is_accessible():
        return False
    try:
        cmdline = ['ssh', vm.host, 'cat', '/etc/timestamp']
        output = processes.log_and_output(cmdline).strip()
        if output != timestamp:
            logging.warning('Actual timestamp %s is not expected %s',
                            output, timestamp)
        return True
    except Exception:
        logging.exception('Failed to get timestamp from %s', vm.host)


def reboot_and_check_test_vm(vmm, vm, timestamp):
    if vm.is_accessible():
        linux_util.reboot(vm.host)
    else:
        logging.warning('%s is not accessble', vm.host)
        vmm.reset(vm)

    wait.wait_for(lambda: booted_properly(vm, timestamp), timeout=180, step=10)


def get_snapshots(vmm, vm):
    pattern = lvm.snapshot_glob(vmm.get_disk(vm))
    return sorted(glob.glob(pattern))


def reboot_inactive_clients(vmm, ref_vm, test_vm):
    snapshots = get_snapshots(vmm, ref_vm)

    for snapshot in snapshots:
        backstore_name = iscsi.get_iscsi_backstore_name(snapshot)
        target_name = iscsi.get_iscsi_target_name(backstore_name)
        sessions = iscsi.get_dynamic_iscsi_sessions(target_name)
        for session in sessions:
            try:
                host = get_hostname(session)
            except Exception:
                logging.exception('Failed to get hostname from %s', session)
                continue
            logging.debug('Snapshot %s is used on %s in session %s',
                          snapshot, host, session)
            if host != test_vm.host:
                linux_util.try_reboot_if_idle(host)


def add_snapshot(args):
    vmm = vm.Virsh()
    ref_vm = vm.LinuxVM(args.ref_vm, args.ref_host)
    test_vm = vm.LinuxVM(args.test_vm, args.test_host)
    cow.check_preconditions(vmm, ref_vm)

    timestamp = cow.generate_timestamp()
    with contextlib.ExitStack() as snapshot_stack:
        snapshot_disk = snapshot_stack.enter_context(vm.vm_disk_snapshot(
            vmm, ref_vm, timestamp, args.snapshot_size, args.cache_config
        ))
        artifacts = snapshot_stack.enter_context(
            cow.snapshot_artifacts(args.output, snapshot_disk)
        )
        logging.info('Snapshot disk is %s', snapshot_disk)
        disk_info = disk.get_disk_information(snapshot_disk)
        assert disk_info.configuration.partition_table_type == 'gpt', (
            'VMs must have disk with GPT partitoin table'
        )
        base_partition = disk.get_partition(snapshot_disk, disk_info,
                                            args.partitions_config.base)
        disk.set_partition_name(snapshot_disk, base_partition.number,
                                args.partitions_config.network)
        disk_info = disk.get_disk_information(snapshot_disk)
        net_partition = disk.get_partition(snapshot_disk, disk_info,
                                           args.partitions_config.network)
        with contextlib.ExitStack() as fs_stack:
            fs_stack.enter_context(disk.partitions_exposed(snapshot_disk))
            root = fs_stack.enter_context(
                mount.chroot(net_partition.kpartx_name)
            )
            mount.copy_files(root, args.to_copy)
            cow.write_timestamp(root, timestamp)
            cow.write_cow_config(args, root)
            cow.run_chroot_script(root, args.chroot_script)
            kernel, initrd = cow.publish_kernel_images(root, artifacts)

        if args.link_snapshot_copy:
            snapshot_stack.enter_context(
                lvm.link_snapshot_copy(
                    snapshot_disk, args.link_snapshot_copy,
                    volume_cache.non_volatile_pv(args.cache_config)
                )
            )

        volume_cache.configure_caching(snapshot_disk, args.cache_config)

        iscsi_target_name = snapshot_stack.enter_context(
            iscsi.publish_to_iscsi(snapshot_disk)
        )
        ipxe_config = snapshot_stack.enter_context(ipxe.generate_ipxe_config(
            args.output, iscsi_target_name, kernel, initrd
        ))

        snapshot_stack.enter_context(vm.reset_back_on_failure(vmm, test_vm))
        snapshot_stack.enter_context(ipxe.published_ipxe_config(
            args.output, ipxe_config, testing=True
        ))
        reboot_and_check_test_vm(vmm, test_vm, timestamp)
        ipxe_config = snapshot_stack.enter_context(
            ipxe.published_ipxe_config(args.output, ipxe_config)
        )
        logging.info('Published iPXE config to %s', ipxe_config)

    if args.push:
        logging.info('Pushing update to inactive clients with reboot')
        reboot_inactive_clients(vmm, ref_vm, test_vm)


def clean_snapshot(output, cache_config, name, force=False):
    backstore_name = iscsi.get_iscsi_backstore_name(name)
    target_name = iscsi.get_iscsi_target_name(backstore_name)
    sessions = iscsi.get_dynamic_iscsi_sessions(target_name)
    if sessions:
        logging.warning('Snapshot %s has the following dynamic sessions:',
                        name)
        for session in sessions:
            logging.warning('  %s', session)
        if not force:
            logging.warning('Skipping cleanup')
            return
        else:
            logging.warning('Continuing as requested')

    ipxe_config = ipxe.ipxe_config_filename(output, target_name)
    if os.path.exists(ipxe_config):
        logging.info('Cleaning iPXE config at %s', ipxe_config)
        os.remove(ipxe_config)

    artifacts = cow.snapshot_artifacts_path(output, name)
    if os.path.exists(artifacts):
        logging.info('Cleaning snapshot artifacts at %s', artifacts)
        shutil.rmtree(artifacts)

    try:
        iscsi.remove_iscsi_target(target_name)
    except Exception:
        logging.warning('Failed to remove iSCSI target %s', target_name)

    try:
        iscsi.remove_iscsi_backstore(backstore_name)
    except Exception:
        logging.warning('Failed to remove iSCSI backstore %s', backstore_name)

    iscsi.save_iscsi_config()

    disk.cleanup_kpartx(name)

    if cache_config:
        volume_cache.delete_cache_record(cache_config, name)

    copy_name = lvm.snapshot_copy_name(name)
    if os.path.exists(copy_name):
        logging.info('Removing snapshot copy %s', copy_name)
        try:
            lvm.remove_lv(copy_name)
        except Exception:
            logging.warning('Failed to remove snapshot copy %s', copy_name)

    if lvm.is_lv_open(name):
        raise RuntimeError(f'LV {name} is still open')

    logging.info('LV %s is not open, proceeding with remove', name)
    lvm.remove_lv(name)

    cache_volume = volume_cache.cache_lv_name(name)
    if os.path.exists(cache_volume):
        logging.warning('Cache volume %s still exists, removing', cache_volume)
        lvm.remove_lv(cache_volume)


def clean_snapshots(args):
    vmm = vm.Virsh()
    ref_vm = vm.LinuxVM(args.ref_vm, None)
    snapshots = get_snapshots(vmm, ref_vm)
    if not snapshots:
        return

    old, latest = snapshots[:-1], snapshots[-1]

    for snapshot in old:
        clean_snapshot(args.output, args.cache_config, snapshot,
                       force=args.force_old)

    if args.force_latest:
        logging.warning('Removing latest snapshot %s', latest)
        clean_snapshot(args.output, args.cache_config, latest, force=True)


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
        '--cache-config',
        type=config.prepare_config_of(type_=volume_cache.CacheConfig)
    )
    add_parser.add_argument('--to-copy', action='append')
    add_parser.add_argument('--chroot-script')
    add_parser.add_argument('--link-snapshot-copy',
                            help='Add symlink to snapshot suitable for '
                                 'deploying to this locaiton')
    add_parser.add_argument('--push', action='store_true',
                            help='Try to push update to inactive clients')

    add_parser.add_argument('ref_vm')
    add_parser.add_argument('ref_host')
    add_parser.add_argument(
        'partitions_config',
        type=config.prepare_config_of(type_=cow.CowPartitionsConfig)
    )
    add_parser.add_argument('output')
    add_parser.add_argument('test_vm')
    add_parser.add_argument('test_host')
    add_parser.set_defaults(func=add_snapshot)

    clean_parser = subparsers.add_parser('clean', help='Cleanup old snapshots')
    clean_parser.add_argument('--force-old', action='store_true')
    clean_parser.add_argument('--force-latest', action='store_true')
    clean_parser.add_argument(
        '--cache-config',
        type=config.prepare_config_of(type_=volume_cache.CacheConfig)
    )
    clean_parser.add_argument('ref_vm')
    clean_parser.add_argument('output')
    clean_parser.set_defaults(func=clean_snapshots)

    enable_cache_parser = subparsers.add_parser(
        'enable_cache',
        help='Add cache PV to VG and enable cache for all the '
             'volumes configured to use with cache'
    )
    enable_cache_parser.add_argument(
        'cache_config',
        type=config.prepare_config_of(type_=volume_cache.CacheConfig)
    )
    enable_cache_parser.add_argument(
        '--cleanup', action='store_true',
        help='Shrink cached VG and uncache previously cached volumes, '
             'if necessary, useful for system startup scripts'
    )
    enable_cache_parser.set_defaults(
        func=lambda args: volume_cache.enable_cache(args.cache_config,
                                                    args.cleanup)
    )

    disable_cache_parser = subparsers.add_parser(
        'disable_cache',
        help='Disable cache for all the cached volumes configured and '
             'remove cache PV out of VG'
    )
    disable_cache_parser.add_argument(
        'cache_config',
        type=config.prepare_config_of(type_=volume_cache.CacheConfig)
    )
    disable_cache_parser.set_defaults(
        func=lambda args: volume_cache.disable_cache(args.cache_config)
    )

    return parser.parse_args(config.get_args(raw_args))


def main(raw_args):
    args = parse_args(raw_args)
    log.configure_logging(args)
    with lock.locked(args):
        return args.func(args)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
