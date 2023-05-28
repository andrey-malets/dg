import contextlib
from dataclasses import dataclass
import datetime
import logging
import os
import shutil

from dg.prepare.util import linux
from dg.prepare.util import processes


@dataclass(frozen=True)
class CowPartitionsConfig:
    base: str
    network: str
    local: str
    cow: str
    conf: str
    sign: str
    keyimage: str
    place: str


def check_preconditions(vmm, ref_vm, ref_host):
    if not vmm.is_vm_running(ref_vm):
        raise RuntimeError(f'Reference vm {ref_vm} is not running')

    if not linux.is_accessible(ref_host):
        raise RuntimeError(f'Reference host {ref_host} is not accessible '
                           'with ssh')


def generate_timestamp():
    return datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')


def write_timestamp(root, timestamp):
    with open(os.path.join(root, 'etc', 'timestamp'), 'w') as timestamp_out:
        print(timestamp, file=timestamp_out)


def write_cow_config(args, root):
    config_path = os.path.join(root, 'etc', 'cow.conf')
    logging.info('Writing cow config to %s', config_path)
    with open(config_path, 'w') as config_output:
        PARTITION_NAMES = 'PARTITION_NAMES'
        config_output.write(f'declare -A {PARTITION_NAMES}\n')
        for key, value in vars(args.partitions_config).items():
            config_output.write(f'{PARTITION_NAMES}[{key}]={value}\n')


def run_chroot_script(root, script):
    if script is not None:
        logging.info('Running chroot script %s in %s', script, root)
        processes.log_and_call(['chroot', root, script])


def snapshot_artifacts_path(output, snapshot_disk):
    return os.path.join(output, os.path.basename(snapshot_disk))


@contextlib.contextmanager
def snapshot_artifacts(output, snapshot_disk):
    path = snapshot_artifacts_path(output, snapshot_disk)
    assert not os.path.exists(path)
    logging.info('Creating snapshot artifacts directory %s', path)
    os.makedirs(path)
    try:
        yield path
    except Exception:
        logging.error('Exception while using artifacts directory %s, '
                      'clening up', path)
        shutil.rmtree(path)
        raise


def publish_kernel_images(root, artifacts):
    logging.info('Publishing kernel images to %s', artifacts)
    return tuple(
        shutil.copy2(os.path.join(root, file_), artifacts)
        for file_ in ('vmlinuz', 'initrd.img')
    )
