import contextlib
import datetime
import logging
import os
import subprocess

from dg.win.prepare import util


def lv_is_free(device):
    cmd = ['lvs', '--noheadings', '-o', 'lv_attr', device]
    output = subprocess.check_output(cmd, text=True).strip()
    open_flag = output[5]
    if open_flag == '-':
        return True
    else:
        assert open_flag == 'o'
        return False


def wait_for_lv_to_free(device, total, step=datetime.timedelta(seconds=10)):
    logging.info('waiting for {} to free'.format(device))
    return util.wait_for_condition(
        total=total, step=step,
        check=lambda: lv_is_free(device),
        step_msg='{} is still open'.format(device),
        fail_msg='timed out while waiting for {} to free'.format(device))


def get_snapshot_name(volume):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    name = '{}-at-{}'.format(volume, timestamp)
    assert not os.path.exists(name)
    return name


@contextlib.contextmanager
def snapshotted_volume(volume, snapshot_size, name_fn=get_snapshot_name,
                       clean=False):
    snapshot_name = name_fn(volume)
    logging.info('Making snapshot %s of %s', snapshot_name, volume)
    subprocess.check_call(
        ['lvcreate', '-s', '-L', snapshot_size, '-n', snapshot_name, volume])
    try:
        yield snapshot_name
    except Exception:
        logging.warning('Cleaning up snapshot %s', snapshot_name)
        subprocess.check_call(['lvremove', '-f', snapshot_name])
        raise
    finally:
        if clean and os.path.exists(snapshot_name):
            logging.info('Cleaning up snapshot %s', snapshot_name)
            subprocess.check_call(['lvremove', '-f', snapshot_name])
