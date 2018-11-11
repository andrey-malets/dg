#!/usr/bin/env python

import argparse
import datetime
import logging
import os
import os.path
import subprocess
import sys
import time


class Timeouts:
    SMALL = datetime.timedelta(minutes=1)
    BIG = datetime.timedelta(minutes=10)


def setup_logging():
    logging.basicConfig(
        format=('%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - '
                '%(message)s'),
        level=logging.INFO)


def parse_xl_config(filename):
    config = {}
    with open(filename) as config_file:
        exec(config_file.read(), config)
    return config


def get_disks(config):
    disks = config['disk']
    hdds = []
    for spec in disks:
        parts = spec.split(',')
        assert len(parts) == 4
        if 'cdrom' not in parts[3]:
            hdds.append(spec)
    return hdds


def get_snapshot_disk_spec(disk, snapshot):
    parts = disk.split(',')
    parts[0] = snapshot
    return ','.join(parts)


def get_snapshot_path(disk):
    basename = disk.split(',')[0]
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return '{}-at-{}'.format(basename, timestamp)


def write_snapshot_config(config, name, disks, output, should_exist=False):
    if not should_exist:
        assert not os.path.isfile(output), '{} already exists'.format(output)
    with open(output, 'w') as outfile:
        outfile.write('name="{}"\n'.format(name))
        for key in ['builder', 'memory', 'boot', 'vif', 'cpus', 'vcpus',
                    'localtime', 'vncconsole', 'vnc', 'vnclisten']:
            if key in config:
                value = config[key]
                fmt = '{}="{}"\n' if type(value) is str else '{}={}\n'
                outfile.write(fmt.format(key, config[key]))
        disk_spec = ','.join('"{}"'.format(disk) for disk in disks)
        outfile.write('disk=[{}]\n'.format(disk_spec))


def wait_for_condition(total, step, check, step_msg, fail_msg):
    start = datetime.datetime.now()
    while datetime.datetime.now() - start < total:
        if check():
            return True
        else:
            logging.info('{}, sleeping for {} seconds'.format(
                step_msg, step.seconds))
            time.sleep(step.seconds)
    logging.error(fail_msg)
    return False


def lv_is_free(device):
    output = subprocess.check_output(
        ['lvs', '--noheadings', '-o', 'lv_attr', device]).strip()
    open_flag = output[5]
    if open_flag == '-':
        return True
    else:
        assert open_flag == 'o'
        return False


def wait_for_lv_to_free(device, total, step=datetime.timedelta(seconds=10)):
    logging.info('waiting for {} to free'.format(device))
    return wait_for_condition(
        total=total, step=step,
        check=lambda: lv_is_free(device),
        step_msg='{} is still open'.format(device),
        fail_msg='timed out while waiting for {} to free'.format(device))


class SSHClient(object):
    def __init__(self, host, login):
        self.host = host
        self.login = login

    def ssh(self, *cmd):
        cmdline = ['ssh', '-o', 'ConnectTimeout=3',
                   '-l', self.login, self.host] + list(cmd)
        logging.info('running {}'.format(cmdline))
        return subprocess.check_output(cmdline)

    def scp(self, src, dest):
        cmdline = ['scp', '-o', 'ConnectTimeout=5',
                   src, '{}@{}:{}'.format(self.login, self.host, dest)]
        logging.info('running {}'.format(cmdline))
        subprocess.check_call(cmdline)

    def is_ready(self):
        try:
            self.ssh('exit')
            return True
        except subprocess.CalledProcessError:
            return False


def wait_for_ssh(client, total, step=datetime.timedelta(seconds=3)):
    logging.info('waiting for ssh to come up on {}'.format(client.host))
    return wait_for_condition(
        total=total, step=step,
        check=lambda: client.is_ready(),
        step_msg='{}@{} is not accessible yet'.format(
            client.login, client.host),
        fail_msg=('timed out while waiting for {}@{} '
                  'to become available'.format(client.login, client.host)))


def copy_setup_scripts(client, scripts):
    dest_dir = '/cygdrive/c/Windows/Setup/Scripts'
    client.ssh('mkdir', '-p', dest_dir)
    for script in scripts:
        logging.info('copying {} to {}'.format(script, dest_dir))
        dest_file = os.path.basename(script)
        client.scp(script, '{}/{}'.format(dest_dir, dest_file))


def start_sysprep(client, sysprep_xml):
    logging.info('starting sysprep with {}'.format(sysprep_xml))
    sysprep = r'C:\\Windows\\system32\\sysprep\\sysprep.exe'
    cygwin_path = '/cygdrive/c/Users/{}/sysprep.xml'.format(client.login)
    windows_path = r'C:\\Users\\{}\\sysprep.xml'.format(client.login)
    client.scp(sysprep_xml, cygwin_path)
    client.ssh('cmd', '/c', 'start', '/w',
               sysprep, '/oobe', '/generalize', '/shutdown',
               '/unattend:{}'.format(windows_path))


def collect_installed_software(client, output):
    client.ssh('wmic', '/output:soft.txt', 'product', 'get', 'name')
    raw_apps = map(
        lambda line: line.strip(),
        client.ssh('cat', 'soft.txt').decode('utf-16le').splitlines())
    apps = sorted(filter(lambda line: len(line) > 0, raw_apps[1:]))
    with open(output, 'w') as output_file:
        for app in apps:
            output_file.write('{}\n'.format(app.encode('utf-8')))


def main(raw_args):
    setup_logging()

    parser = argparse.ArgumentParser(
        description='Prepare Windows 7 snapshot with sysprep and LVM')

    parser.add_argument('CONFIG', help='path to ref VM config file')
    parser.add_argument('SNAP_CONFIG',
                        help='path to snapshot config file for prepared VM')
    parser.add_argument(
        '-d', metavar='INDEX', type=int, default=0,
        help='Index of VM disk in config file to make snapshot of')
    parser.add_argument(
        '-H', metavar='HOST', help='Machine hostname for SSH connections')
    parser.add_argument(
        '-u', metavar='LOGIN', help='Username for SSH connections',
        default='Administrator')
    parser.add_argument(
        '-s', metavar='SIZE', help='Snapshot size for sysprep', default='20G')

    parser.add_argument(
        '-S', metavar='SYSPREP.XML',
        help='Sysprep unattended file location', required=True)
    parser.add_argument(
        '-ss', metavar='SCRIPT', default=[], action='append',
        help='setup scripts to copy to Windows\Setup\Scripts')
    parser.add_argument(
        '-l', metavar='PKGS', help='Filename for installed software list',
        required=True)
    parser.add_argument(
        '-p', metavar='RESULT',
        help='Location where link to resulting image will be put',
        required=True)

    parser.add_argument(
        '-t', help='Test mode: start vm from resulting image at the end',
        action='store_true', default=False)

    args = parser.parse_args(raw_args)

    config = parse_xl_config(args.CONFIG)
    vm_name = config['name']
    disks = get_disks(config)
    disk = disks[args.d]
    disk_path = disk.split(',')[0]
    snapshot_path = get_snapshot_path(disk)
    logging.info('snapshot path is {}'.format(snapshot_path))
    disks[args.d] = get_snapshot_disk_spec(disk, snapshot_path)

    snapshot_name = os.path.basename(snapshot_path)
    snapshot_vm_name = '{}-snap'.format(vm_name)
    write_snapshot_config(config, snapshot_vm_name, disks, args.SNAP_CONFIG)
    try:
        host = args.H if args.H is not None else vm_name
        client = SSHClient(host, args.u)
        assert wait_for_ssh(client, Timeouts.SMALL)

        logging.info('collecting installed software to {}'.format(args.l))
        collect_installed_software(client, args.l)

        logging.info('shutting down {}'.format(vm_name))
        client.ssh('shutdown', '/s', '/t', '0')
        assert wait_for_lv_to_free(disk_path, Timeouts.BIG)

        logging.info('creating snapshot volume {}'.format(snapshot_name))
        subprocess.check_call(
            ['lvcreate', '-s', '-L', args.s, '-n', snapshot_name, disk_path])

        logging.info('starting {} from snapshot'.format(snapshot_vm_name))
        subprocess.check_call(['xl', 'create', args.SNAP_CONFIG])
        assert wait_for_ssh(client, Timeouts.BIG)

        copy_setup_scripts(client, args.ss)
        start_sysprep(client, args.S)
        assert wait_for_lv_to_free(snapshot_path, Timeouts.BIG)

        logging.info('publishing image to {}'.format(args.p))
        pmap = subprocess.check_output(['kpartx', '-l', snapshot_path])
        subprocess.check_call(['kpartx', '-a', snapshot_path])
        partition = '/dev/mapper/{}'.format(
            pmap.strip().splitlines()[-1].split(' ', 1)[0])
        if os.path.lexists(args.p):
            os.unlink(args.p)
        os.symlink(partition, args.p)

        if args.t:
            test_vm_name = '{}-test'.format(vm_name)
            logging.warning('starting vm "{}" for test'.format(test_vm_name))
            write_snapshot_config(config, test_vm_name, disks,
                                  args.SNAP_CONFIG, should_exist=True)
            subprocess.check_call(['xl', 'create', args.SNAP_CONFIG])
        else:
            logging.info('starting {} back'.format(vm_name))
            subprocess.check_call(['xl', 'create', args.CONFIG])
    finally:
        os.remove(args.SNAP_CONFIG)

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
