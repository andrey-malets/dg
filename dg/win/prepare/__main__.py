import argparse
import contextlib2
import datetime
import logging
import os
import sys

from dg.win.prepare import lv
from dg.win.prepare import ssh
from dg.win.prepare import sysprep
from dg.win.prepare import util
from dg.win.prepare import vm


STARTUP_TIMEOUT = datetime.timedelta(minutes=5)


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

    return args


def main(raw_args):
    args = parse_args(raw_args)
    ref_vm_config = util.parse_xl_config(args.CONFIG)
    ref_vm_disk = util.get_hard_disks(ref_vm_config)[args.d]

    ssh_client = ssh.SSHClient(args.HOST, args.u)
    ssh_client.wait_ssh_ready(STARTUP_TIMEOUT)

    def shutdown():
        ssh_client.ssh('shutdown', '/s', '/t', '0')

    with contextlib2.ExitStack() as stack:
        stack.enter_context(vm.xen_vm_shut_down(shutdown, args.CONFIG))
        disk_snapshot_name = stack.enter_context(
            lv.snapshotted_volume(ref_vm_disk, args.s))
        snapshot_vm_name = '{}-snapshot'.format(ref_vm_config['name'])
        stack.enter_context(vm.xen_vm_with_alternate_disk(
            args.CONFIG, snapshot_vm_name, args.d, disk_snapshot_name))
        ssh_client.wait_ssh_ready(STARTUP_TIMEOUT)
        sysprep.copy_setup_scripts(ssh_client, args.ss)
        sysprep.start_sysprep(ssh_client, args.SYSPREP_XML)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s][%(levelname).1s] %(message)s',
        datetfmt='%H:%M:%S')

    sys.exit(main(sys.argv[1:]))
