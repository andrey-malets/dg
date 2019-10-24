import argparse
import contextlib2
import datetime
import logging
import os
import subprocess
import sys

from dg.win.prepare import lv
from dg.win.prepare import ssh
from dg.win.prepare import sysprep
from dg.win.prepare import util
from dg.win.prepare import vm


STARTUP_TIMEOUT = datetime.timedelta(minutes=5)
SYSPREP_START_TIMEOUT = datetime.timedelta(minutes=5)
SPECIALIZE_TIMEOUT = datetime.timedelta(minutes=15)


def parse_args(raw_args):
    parser = argparse.ArgumentParser(
        description='Prepare Windows snapshot from XEN virtual machine '
                    'with sysprep and LVM')
    parser.add_argument('-d', metavar='INDEX', type=int, default=0,
                        help='Zero-based VM disk index to make snapshot of, '
                             'default is 0')
    parser.add_argument('-s', metavar='SIZE', default='20G',
                        help='Snapshot size for sysprep, default is 20G')

    parser.add_argument('-u', metavar='LOGIN', default='Administrator',
                        help='Username for SSH connections, default is '
                             '"Administrator"')

    parser.add_argument('-t', action='store_true',
                        help='Test mode: start VM from the snapshot after '
                             'sysprep and wait until it comes back up')
    parser.add_argument('-l', metavar='LINK',
                        help='Make a symlink from LINK to snapshot volume. '
                             'If LINK already exists, it must be a symlink '
                             'pointing to the snapshot done before, in which '
                             'case it will be overwritten, and the old '
                             'snapshot deleted')

    parser.add_argument('CONFIG', help='path to ref VM config file')
    parser.add_argument('HOST', help='host use for SSH connections')

    parser.add_argument(
        '-ss', metavar='SCRIPT', default=[], nargs='*',
        help=r'setup script(s) to copy to Windows\Setup\Scripts')
    parser.add_argument('SYSPREP_XML', metavar='SYSPREP.XML',
                        help='Path to sysprep.xml file to use')

    args = parser.parse_args(raw_args)
    for path in (args.CONFIG, args.SYSPREP_XML):
        if not os.path.isfile(path):
            parser.error('{} does not exist'.format(path))
    if args.l:
        if args.t:
            parser.error('test mode cannot be used with making snapshot '
                         'symlink, please use either "-l" or "-t"')
        if os.path.exists(args.l):
            if not os.path.islink(args.l):
                parser.error('{} is not a symbolic link'.format(args.l))
            try:
                subprocess.check_output(['lvdisplay', os.readlink(args.l)])
            except subprocess.CalledProcessError:
                parser.error('{} does not appear to be '
                             'LVM logical volume'.format(args.l))

    return args


def switch_symlink(link, new_snapshot):
    if not link:
        return

    link_existed = os.path.exists(link)

    if link_existed:
        old_snapshot = os.readlink(link)
        logging.info('Switching snapshot link at %s from %s to %s', link,
                     old_snapshot, new_snapshot)
        os.unlink(link)
    else:
        logging.info('Making snapshot link at %s to %s', link, new_snapshot)

    os.symlink(new_snapshot, link)
    if link_existed:
        logging.info('Removing old snapshot %s', old_snapshot)
        subprocess.check_call(['lvremove', '-f', old_snapshot])


def main(raw_args):
    args = parse_args(raw_args)
    ref_vm_config = util.parse_xl_config(args.CONFIG)
    ref_vm_disk = util.get_hard_disks(ref_vm_config)[args.d]

    ssh_client = ssh.SSHClient(args.HOST, args.u)
    ssh_client.wait_ssh_ready(STARTUP_TIMEOUT)

    def shutdown():
        logging.info('Shutdown output: %s', ssh_client.ssh(
            ['shutdown', '/s', '/t', '0']))

    with contextlib2.ExitStack() as stack:
        stack.enter_context(vm.xen_vm_shut_down(shutdown, args.CONFIG))
        disk_snapshot_name = stack.enter_context(
            lv.snapshotted_volume(ref_vm_disk, args.s, clean=args.t))
        snapshot_vm_name = '{}-snapshot'.format(ref_vm_config['name'])
        with vm.xen_vm_with_alternate_disk(args.CONFIG, snapshot_vm_name,
                                           args.d, disk_snapshot_name):
            ssh_client.wait_ssh_ready(STARTUP_TIMEOUT)
            sysprep.copy_setup_scripts(ssh_client, args.ss)
            sysprep.start_sysprep(ssh_client, args.SYSPREP_XML,
                                  SYSPREP_START_TIMEOUT)

        if args.t:
            logging.info('Starting VM from "sysprepped" snapshot for test')
            with vm.xen_vm_with_alternate_disk(args.CONFIG, snapshot_vm_name,
                                               args.d, disk_snapshot_name):
                ssh_client.wait_ssh_ready(SPECIALIZE_TIMEOUT)
                shutdown()
        else:
            switch_symlink(args.l, disk_snapshot_name)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s][%(levelname).1s] %(message)s',
        datetfmt='%H:%M:%S')

    sys.exit(main(sys.argv[1:]))
