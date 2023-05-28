import contextlib
import logging
import os

from dg.prepare.util import processes
from dg.prepare.util import transactions


def remove_iscsi_backstore(name):
    logging.info('Removing iSCSI backstore %s', name)
    processes.log_and_call(['targetcli', '/backstores/block', 'delete', name])


def get_iscsi_backstore_name(device):
    return os.path.basename(device)


@contextlib.contextmanager
def create_iscsi_backstore(device):
    name = get_iscsi_backstore_name(device)
    cmdline = ['targetcli', '/backstores/block', 'create',
               f'dev={device}', f'name={name}', 'readonly=True']
    logging.info('Adding iSCSI backstore %s', name)
    processes.log_and_call(cmdline)
    with transactions.transact(
        rollback=(
            f'cleaning up iSCSI backstore {name}',
            lambda _: remove_iscsi_backstore(name)
        )
    ):
        yield name


def remove_iscsi_target(name):
    logging.info('Removing iSCSI target %s', name)
    processes.log_and_call(['targetcli', '/iscsi', 'delete', name])


def attach_backstore_to_iscsi_target(target_name, backstore_name):
    logging.info('Adding iSCSI LUN to %s from %s', target_name, backstore_name)
    cmdline = ['targetcli', f'/iscsi/{target_name}/tpg1/luns', 'create',
               f'/backstores/block/{backstore_name}']
    processes.log_and_call(cmdline)


def get_iscsi_target_name(backstore_name):
    return f'iqn.2013-07.cow.{backstore_name}'


@contextlib.contextmanager
def create_iscsi_target(backstore_name):
    target_name = get_iscsi_target_name(backstore_name)
    logging.info('Adding iSCSI target %s', target_name)
    processes.log_and_call(['targetcli', '/iscsi', 'create', target_name])

    with transactions.transact(
        rollback=(
            f'cleaning up iSCSI target {target_name}',
            lambda _: remove_iscsi_target(target_name)
        )
    ):
        attach_backstore_to_iscsi_target(target_name, backstore_name)
        yield target_name


def configure_authentication(target_name):
    cmdline = ['targetcli', f'/iscsi/{target_name}/tpg1', 'set', 'attribute',
               'generate_node_acls=1']
    logging.info('Configuring iSCSI authentication')
    processes.log_and_call(cmdline)


def save_iscsi_config():
    logging.info('Saving iSCSI configuration')
    processes.log_and_call(['targetcli', 'saveconfig'])


@contextlib.contextmanager
def publish_to_iscsi(device):
    with transactions.transact(
        rollback=('saving iSCSI config', lambda _: save_iscsi_config())
    ), contextlib.ExitStack() as stack:
        backstore_name = stack.enter_context(create_iscsi_backstore(device))
        target_name = stack.enter_context(create_iscsi_target(backstore_name))
        configure_authentication(target_name)
        save_iscsi_config()
        yield target_name


def get_dynamic_iscsi_sessions(target_name):
    dynamic_sessions_file = os.path.join(
        '/sys/kernel/config/target/iscsi',
        target_name, 'tpgt_1/dynamic_sessions'
    )
    if not os.path.exists(dynamic_sessions_file):
        return []

    with open(dynamic_sessions_file) as sessions_input:
        lines = sessions_input.read().split('\0')
        return list(filter(None, map(str.strip, lines)))
