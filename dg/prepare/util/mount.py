import contextlib
import logging
import os
import shutil
import tempfile

from dg.prepare.util import processes
from dg.prepare.util import transactions


def umount(mountpoint):
    processes.log_and_call(['umount', mountpoint])


@contextlib.contextmanager
def mounted(device, mountpoint, type_=None, options=None):
    assert os.path.exists(mountpoint), f'{mountpoint} does not exist'

    mount_cmdline = ['mount']
    if type_ is not None:
        mount_cmdline.extend(['-t', type_])
    if options is not None:
        mount_cmdline.extend(options)
    mount_cmdline.append('none' if device is None else device)
    mount_cmdline.append(mountpoint)

    logging.info('Mounting %s to %s', device, mountpoint)
    processes.log_and_call(mount_cmdline)

    with transactions.transact(
            final=(f'unmouning {mountpoint}',
                   lambda _: processes.log_and_call(['umount', mountpoint]))
    ):
        yield


@contextlib.contextmanager
def chroot(partition):
    with contextlib.ExitStack() as stack:
        root = stack.enter_context(
            tempfile.TemporaryDirectory(prefix='snapshot_root_')
        )
        stack.enter_context(mounted(partition, root))
        stack.enter_context(mounted(None, os.path.join(root, 'proc'),
                                    type_='proc'))
        stack.enter_context(mounted(None, os.path.join(root, 'sys'),
                                    type_='sysfs'))
        stack.enter_context(mounted('/dev', os.path.join(root, 'dev'),
                                    options=('--bind',)))
        stack.enter_context(mounted('/dev/pts',
                                    os.path.join(root, 'dev', 'pts'),
                                    options=('--bind',)))
        yield root


def copy_files(root, to_copy):
    def relpath(top, dirpath, path):
        return os.path.relpath(os.path.join(dirpath, path), top)

    for dir_ in to_copy:
        logging.info('Copying contents of %s to %s', dir_, root)
        assert os.path.isdir(dir_)
        for dirpath, dirnames, filenames in os.walk(dir_):
            for dirname in dirnames:
                dst = os.path.join(root, relpath(dir_, dirpath, dirname))
                os.makedirs(dst, exist_ok=True)
            for filename in filenames:
                src = os.path.join(dirpath, filename)
                dst = os.path.join(root, relpath(dir_, dirpath, filename))
                if os.path.exists(dst):
                    logging.debug('Overwriting %s with %s', dst, src)
                else:
                    logging.debug('Copying %s to %s', src, dst)
                shutil.copy2(src, dst)
