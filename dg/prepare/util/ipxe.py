import contextlib
import logging
import os
import socket

from dg.prepare.util import transactions


def ipxe_config_filename(output, iscsi_target_name):
    return os.path.join(output, f'{iscsi_target_name}.ipxe')


@contextlib.contextmanager
def generate_ipxe_config(output, iscsi_target_name, kernel, initrd):
    kernel_path = os.path.relpath(kernel, output)
    initrd_path = os.path.relpath(initrd, output)
    config_path = ipxe_config_filename(output, iscsi_target_name)
    with open(config_path, 'w') as config_output:
        config_output.write('\n'.join([
            '#!ipxe',
            '',
            f'set iti {socket.getfqdn()}',
            f'set itn {iscsi_target_name}',
            (
                'set iscsi_params '
                'iscsi_target_ip=${iti} iscsi_target_name=${itn}',
            ),
            (
                'set cow_params '
                'cowsrc=network cowtype=${cowtype} root=/dev/mapper/root '
                '${console}'
            ),
            (
                'set params '
                '${iscsi_params} ${cow_params}'
            ),
            f'kernel {kernel_path} BOOTIF=01-${{netX/mac}} ${{params}} quiet',
            f'initrd {initrd_path}',
            'boot',
            '',
        ]))

    with transactions.transact(
        rollback=(
            f'cleaning up iSCSI config {config_path}',
            lambda _: os.remove(config_path)
        )
    ):
        yield config_path


@contextlib.contextmanager
def saved_config(path):
    old_path = f'{path}.old'
    if os.path.exists(old_path):
        logging.warning('Old config %s exists, removing', old_path)
        os.remove(old_path)

    if not os.path.exists(path):
        logging.warning('%s does not exist', path)
    else:
        os.rename(path, old_path)

    try:
        yield old_path
    except Exception:
        logging.warning('Restoring config %s from %s', path, old_path)
        if os.path.exists(old_path):
            os.rename(old_path, path)
        raise
    else:
        os.remove(old_path)


@contextlib.contextmanager
def published_ipxe_config(output, config, testing=False):
    path = os.path.join(output, 'boot-test.ipxe' if testing else 'boot.ipxe')
    logging.info(f'Publishing{" testing" if testing else ""} iPXE config '
                 'to %s', path)
    with contextlib.ExitStack() as stack:
        stack.enter_context(saved_config(path))
        stack.enter_context(transactions.transact(
            rollback=(f'removing {path}', lambda _: os.remove(path))
        ))
        os.symlink(config, path)
        yield path
