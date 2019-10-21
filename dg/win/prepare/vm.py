import contextlib
import datetime
import logging
import subprocess

from dg.win.prepare import lv
from dg.win.prepare import util


DISK_FREE_TIMEOUT = datetime.timedelta(minutes=10)


@contextlib.contextmanager
def xen_vm_shut_down(shutdown_cmd, vm_config_filename):
    xl_config = util.parse_xl_config(vm_config_filename)
    disks = util.get_hard_disks(xl_config)
    name = xl_config['name']

    logging.info('Shutting down vm "%s"', name)
    shutdown_cmd()

    for disk in disks:
        lv.wait_for_lv_to_free(disk, DISK_FREE_TIMEOUT)
    try:
        yield
    finally:
        for disk in disks:
            assert lv.lv_is_free(disk)

        logging.info('Starting "{}" back'.format(name))
        subprocess.check_call(['xl', 'create', vm_config_filename])


@contextlib.contextmanager
def xen_vm_with_alternate_disk(vm_config_filename, name, index, disk):
    xl_config = util.parse_xl_config(vm_config_filename)
    orig_name = util.change_name(xl_config, name)
    util.change_disk(xl_config, index, disk)

    with util.temporary_file(prefix='{}_'.format(orig_name),
                             suffix='.cfg') as snapshot_config_filename:
        logging.info('Writing snapshot config to %s', snapshot_config_filename)
        util.write_xl_config(xl_config, snapshot_config_filename)

        logging.info('Starting %s from %s', name, snapshot_config_filename)
        subprocess.check_call(['xl', 'create', snapshot_config_filename])

        try:
            yield
        except Exception:
            logging.warning('Trying to destroy %s', name)
            subprocess.call(['xl', 'destroy', name])
            raise
        finally:
            for disk in util.get_hard_disks(xl_config):
                lv.wait_for_lv_to_free(disk, DISK_FREE_TIMEOUT)
