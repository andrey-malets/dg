import contextlib
import logging
import os

from dg.prepare.util import processes
from dg.prepare.util import transactions


LVM_SNAPSHOT_SUFFIX = '-snapshot'


def lvm_snapshot_name(origin, timestamp):
    return f'{os.path.basename(origin)}-at-{timestamp}'


def vm_snapshot_name(lvm_snapshot_name):
    return f'{lvm_snapshot_name}-snapshot'


def snapshot_copy_name(vm_snapshot_name):
    return f'{vm_snapshot_name}-copy'


def lv_path(vg, lv):
    return f'/dev/{vg}/{lv}'


def snapshot_glob(origin):
    return f'{origin}-at-*-snapshot'


def is_lv_open(name):
    logging.info('Checking if LV %s is open', name)
    cmdline = ['lvs', '-o', 'lv_attr', '--noheadings', name]
    output = processes.log_and_output(cmdline).strip()
    flag = output[5]
    if flag == '-':
        return False
    elif flag == 'o':
        return True
    else:
        raise RuntimeError(f'Cannot parse LV attributes "{output}"')


def create_lvm_snapshot(origin, name, non_volatile_pv, size=None,
                        extents=None):
    cmdline = ['lvcreate', '-y', '-s', '-n', name]
    if size:
        cmdline.extend(('-L', size))
    else:
        assert extents
        cmdline.extend(('-l', extents))
    cmdline.append(origin)
    if non_volatile_pv is not None:
        cmdline.append(non_volatile_pv)
    processes.log_and_call(cmdline)


def create_lvm_volume(name, size, vg, pv=None):
    create_cmdline = ['lvcreate', '-y', '-L', f'{size}B', '-n', name, vg]
    if pv is not None:
        create_cmdline.append(pv)
    processes.log_and_call(create_cmdline)
    return name


def remove_lv(name):
    processes.log_and_call(['lvremove', '-f', name])


def create_volume_copy(src, dst, non_volatile_pv):
    size = processes.log_and_output(['blockdev', '--getsize64', src]).strip()
    vg = os.path.basename(os.path.dirname(src))
    return os.path.join(
        os.path.dirname(src),
        create_lvm_volume(dst, size, vg, non_volatile_pv)
    )


@contextlib.contextmanager
def volume_copy(src, dst, non_volatile_pv):
    with transactions.transact(
        prepare=(
            f'copying LVM {src} to {dst}',
            lambda: create_volume_copy(src, dst, non_volatile_pv)
        ),
        rollback=(
            'cleaning up LVM copy',
            lambda result: remove_lv(result[0])
        )
    ) as copy_name:
        yield copy_name


def copy_data(src, dst, block_size='128M'):
    logging.info('Copying data from %s to %s', src, dst)
    processes.log_and_call([
        'dd', f'if={src}', f'of={dst}', f'bs={block_size}'
    ])


def move_link(src, dst):
    new_dst = f'{dst}.new'
    if os.path.exists(new_dst):
        logging.waring('%s already exists, removing', new_dst)
        os.unlink(new_dst)
    os.symlink(src, new_dst)
    os.rename(new_dst, dst)


@contextlib.contextmanager
def link_snapshot_copy(origin, copy_to, non_volatile_pv):
    copy_name = snapshot_copy_name(origin)
    with contextlib.ExitStack() as stack:
        copy = stack.enter_context(
            volume_copy(origin, copy_name, non_volatile_pv)
        )
        copy_data(origin, copy)
        stack.enter_context(transactions.transact(
            commit=(
                f'linking snapshot copy {copy_name} to {copy_to}',
                lambda _: move_link(copy, copy_to)
            ),
        ))
        yield
