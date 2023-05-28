import abc
import contextlib
import logging
import os
import xml.etree.ElementTree as ET

from dg.prepare.util import linux
from dg.prepare.util import lvm
from dg.prepare.util import processes
from dg.prepare.util import transactions
from dg.prepare.util import volume_cache
from dg.prepare.util import wait


class VirtualMachineManager(abc.ABC):

    def is_vm_running(self, name):
        pass

    def start(self, name):
        pass

    def reset(self, name):
        pass

    def get_disks(self, name):
        pass


class Virsh(VirtualMachineManager):

    def is_vm_running(self, name):
        logging.info('Checking if %s is running', name)
        cmdline = ['virsh', 'list', '--state-running', '--name']
        list_output = processes.log_and_output(cmdline)
        domains = set(d.strip() for d in list_output.splitlines() if d)
        logging.info('Running domains: %s', domains)

        return name in domains

    def start(self, name):
        processes.log_and_call(['virsh', 'start', name])

    def reset(self, name):
        logging.warning('Resetting %s', name)
        processes.log_and_call(['virsh', 'reset', name])

    def get_disks(self, name):
        xml = processes.log_and_output(['virsh', 'dumpxml', name])
        root = ET.fromstring(xml)
        for disk in root.findall('./devices/disk/source'):
            yield disk.get('dev')


@contextlib.contextmanager
def vm_shut_down(vmm, name, host):
    linux.shutdown(host)
    wait.wait_for(lambda: not vmm.is_vm_running(name), timeout=180, step=3)
    try:
        yield
    finally:
        vmm.start(name)
        try:
            wait.wait_for(lambda: linux.is_accessible(host), 300, 5)
        except wait.Timeout:
            logging.exception('Timed out waiting for %s to become accessbile '
                              'with ssh', host)
            raise


def get_disk(vmm, vm):
    disks = list(vmm.get_disks(vm))
    if len(disks) != 1:
        raise RuntimeError('Need exactly one disk for vm, got {disks}')
    return disks[0]


def create_vm_disk_snapshot(vmm, vm, host, timestamp, size, non_volatile_pv):
    origin = None
    name = None
    with vm_shut_down(vmm, vm, host):
        lv = get_disk(vmm, vm)
        wait.wait_for(lambda: not lvm.is_lv_open(lv), timeout=30, step=1)
        origin = lv
        name = lvm.lvm_snapshot_name(origin, timestamp)
        lvm.create_lvm_snapshot(origin, name, non_volatile_pv, size=size)

    return os.path.join(os.path.dirname(origin), name)


@contextlib.contextmanager
def vm_disk_snapshot(vmm, ref_vm, ref_host, timestamp, size, cache_config):
    nvpv = volume_cache.non_volatile_pv(cache_config)
    with contextlib.ExitStack() as stack:
        with transactions.transact(
            prepare=(
                f'Creating disk snapshot of {ref_vm}',
                lambda: create_vm_disk_snapshot(vmm, ref_vm, ref_host,
                                                timestamp, size, nvpv)
            ),
            final=(
                'cleaning up disk snapshot',
                lambda result: lvm.remove_lv(result[0])
            )
        ) as lvm_snapshot:
            assert os.path.exists(lvm_snapshot)
            vm_snapshot = stack.enter_context(lvm.volume_copy(
                lvm_snapshot,
                lvm.vm_snapshot_name(os.path.basename(lvm_snapshot)),
                nvpv
            ))
            assert os.path.exists(vm_snapshot)
            lvm.copy_data(lvm_snapshot, vm_snapshot)
        yield vm_snapshot


@contextlib.contextmanager
def reset_back_on_failure(vmm, vm):
    with transactions.transact(rollback=(None, lambda _: vmm.reset(vm))):
        yield
