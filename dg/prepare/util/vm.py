import abc
import contextlib
import logging
import os
import stat
import sys
import tempfile
import xml.etree.ElementTree as ET

from dg.prepare.util import linux
from dg.prepare.util import lvm
from dg.prepare.util import processes
from dg.prepare.util import transactions
from dg.prepare.util import volume_cache
from dg.prepare.util import wait
from dg.prepare.util import windows


class VirtualMachine(abc.ABC):

    def __init__(self, name, host):
        self.name = name
        self.host = host

    @abc.abstractmethod
    def is_accessible(self):
        pass

    @abc.abstractmethod
    def shutdown(self):
        pass


class LinuxVM(VirtualMachine):

    def is_accessible(self):
        return linux.is_accessible(self.host)

    def shutdown(self):
        return linux.shutdown(self.host)


class WindowsVM(VirtualMachine):

    def is_accessible(self):
        return windows.is_accessible(self.host)

    def shutdown(self):
        return windows.shutdown(self.host)


class VirtualMachineManager(abc.ABC):

    def is_vm_running(self, vm):
        pass

    def start(self, vm):
        pass

    def destroy(self, vm):
        pass

    def reset(self, vm):
        pass

    def get_disk(self, vm):
        pass

    def set_disk(self, vm, old_disk, disk):
        pass


DISK_NODE = './devices/disk/source'


@contextlib.contextmanager
def set_disk_script(old_disk, disk):
    script_content = f"""#!{sys.executable}

import sys
import xml.etree.ElementTree as ET

tree = ET.parse(sys.argv[1])
nodes = tree.findall('{DISK_NODE}')
assert len(nodes) == 1, (
    f'expected exactly one node with selector "{DISK_NODE}", got {{nodes}}'
)
assert nodes[0].get('dev') == '{old_disk}', (
    f"expected disk dev to be {old_disk}, got {{nodes[0].get('dev')}}"
)
nodes[0].set('dev', '{disk}')
tree.write(sys.argv[1])
"""
    with tempfile.NamedTemporaryFile(
            mode='w', prefix='set_disk_', suffix='.py', delete=False
    ) as sds:
        sds.write(script_content)
        sds.close()
        os.chmod(sds.name, stat.S_IRUSR | stat.S_IXUSR)
        try:
            yield sds.name
        finally:
            os.unlink(sds.name)


class Virsh(VirtualMachineManager):

    def is_vm_running(self, vm):
        logging.info('Checking if %s is running', vm.name)
        cmdline = ['virsh', 'list', '--state-running', '--name']
        list_output = processes.log_and_output(cmdline)
        domains = set(d.strip() for d in list_output.splitlines() if d)
        logging.info('Running domains: %s', domains)

        return vm.name in domains

    def start(self, vm):
        processes.log_and_call(['virsh', 'start', vm.name])

    def destroy(self, vm):
        logging.warning('Destroying %s', vm.name)
        processes.log_and_call(['virsh', 'destroy', vm.name])
        wait.wait_for(lambda: not self.is_vm_running(vm), timeout=30, step=3)

    def reset(self, vm):
        logging.warning('Resetting %s', vm.name)
        processes.log_and_call(['virsh', 'reset', vm.name])

    def get_disks(self, vm):
        xml = processes.log_and_output(['virsh', 'dumpxml', vm.name])
        root = ET.fromstring(xml)
        for disk in root.findall(DISK_NODE):
            yield disk.get('dev')

    def get_disk(self, vm):
        disks = list(self.get_disks(vm))
        if len(disks) != 1:
            raise RuntimeError('Need exactly one disk for vm {vm.name}, '
                               'got {disks}')
        return disks[0]

    def set_disk(self, vm, old_disk, disk):
        with set_disk_script(old_disk, disk) as script:
            env = os.environ.copy()
            env['EDITOR'] = script
            processes.log_and_call(['virsh', 'edit', vm.name], env=env)
        return disk


@contextlib.contextmanager
def vm_shut_down(vmm, vm):
    vm.shutdown()
    wait.wait_for(lambda: not vmm.is_vm_running(vm), timeout=180, step=3)
    try:
        yield
    finally:
        vmm.start(vm)
        try:
            wait.wait_for(lambda: vm.is_accessible(), 300, 5)
        except wait.Timeout:
            logging.exception('Timed out waiting for %s to become accessbile '
                              'with ssh', vm.host)
            raise


def create_vm_disk_snapshot(vmm, vm, timestamp, size, non_volatile_pv):
    origin = None
    name = None
    with vm_shut_down(vmm, vm):
        lv = vmm.get_disk(vm)
        wait.wait_for(lambda: not lvm.is_lv_open(lv), timeout=30, step=1)
        origin = lv
        name = lvm.lvm_snapshot_name(origin, timestamp)
        lvm.create_lvm_snapshot(origin, name, non_volatile_pv, size=size)

    return os.path.join(os.path.dirname(origin), name)


@contextlib.contextmanager
def vm_disk_snapshot(vmm, vm, timestamp, size, cache_config):
    nvpv = volume_cache.non_volatile_pv(cache_config)
    with contextlib.ExitStack() as stack:
        with transactions.transact(
            prepare=(
                f'Creating disk snapshot of {vm.name}',
                lambda: create_vm_disk_snapshot(vmm, vm, timestamp, size, nvpv)
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
