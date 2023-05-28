import contextlib
from dataclasses import dataclass
import logging
import os

from dg.prepare.util import processes
from dg.prepare.util import transactions
from dg.prepare.util import lvm


@dataclass(frozen=True)
class CacheConfig:
    volume_group: str
    non_volatile_pv: str
    cache_pv: str
    cache_volume_size: str
    cached_volumes_path: str


def cache_lv_name(vm_snapshot_name):
    return f'{vm_snapshot_name}-cache'


def non_volatile_pv(config):
    return (config.non_volatile_pv if config else None)


def create_cache_volume(non_cached_name, config):
    name = cache_lv_name(non_cached_name)
    logging.info('Adding cache volume %s for %s', non_cached_name, name)
    return lvm.create_lvm_volume(name, config.cache_volume_size,
                                 config.volume_group, config.cache_pv)


@contextlib.contextmanager
def cache_volume(non_cached_name, config):
    with transactions.transact(
        prepare=(
            None,
            lambda: create_cache_volume(non_cached_name, config)
        ),
        rollback=(
            f'removing cache volume for {non_cached_name}',
            lambda result: lvm.remove_lv(result[0])
        )
    ) as cached_name:
        yield cached_name


def cache_record_file(config, volume):
    return os.path.join(config.cached_volumes_path, os.path.basename(volume))


def create_cache_record(config, volume):
    record_file = cache_record_file(config, volume)
    os.makedirs(os.path.dirname(record_file), exist_ok=True)
    with open(record_file, 'w'):
        pass


def delete_cache_record(config, volume):
    record_file = cache_record_file(config, volume)
    try:
        os.remove(record_file)
    except FileNotFoundError:
        logging.warning('Cache record file %s does not exist', record_file)


def list_cache_records(config):
    return os.listdir(config.cached_volumes_path)


@contextlib.contextmanager
def cache_record(name, config):
    with transactions.transact(
        prepare=(
            f'Adding cache record for {name}',
            lambda: create_cache_record(config, name)
        ),
        rollback=(
            f'Deleting cache record for {name}',
            lambda _: delete_cache_record(config, name)
        )
    ):
        yield


def configure_caching(non_cached_volume, config):
    if config is None:
        logging.info('Caching is not configured, skipping cache for %s',
                     non_cached_volume)
        return non_cached_volume
    try:
        with contextlib.ExitStack() as stack:
            cache_volume_name = stack.enter_context(
                cache_volume(non_cached_volume, config)
            )
            stack.enter_context(cache_record(non_cached_volume, config))
            enable_cmdline = [
                'lvconvert', '-y', '--type', 'cache',
                '--cachevol', cache_volume_name,
                '--cachemode', 'writethrough', non_cached_volume
            ]
            logging.info('Enabling cache for %s on %s', non_cached_volume,
                         cache_volume_name)
            processes.log_and_call(enable_cmdline)
            cached_volume = non_cached_volume
            return cached_volume
    except Exception:
        logging.exception('Failed to enable caching for %s', non_cached_volume)
        return non_cached_volume


def disable_cache_on(volume):
    try:
        logging.info('Disabling cache on %s', volume)
        processes.log_and_call(['lvconvert', '--uncache', volume])
    except Exception:
        logging.exception('Failed to disable cache for %s', volume)


def cleanup_cache(config):
    vg = config.volume_group
    for record in list_cache_records(config):
        disable_cache_on(lvm.lv_path(vg, record))

    logging.info('Reducing VG %s, removing missing PVs', vg)
    processes.log_and_call(['vgreduce', '--removemissing', vg])

    logging.info('Activating all LVs in VG %s', vg)
    processes.log_and_call(['vgchange', '-ay', vg])


def enable_cache(config, cleanup):
    if cleanup:
        cleanup_cache(config)

    cache_pv = config.cache_pv
    logging.info('Creating cache PV %s', cache_pv)
    processes.log_and_call(['pvcreate', '-y', cache_pv])

    vg = config.volume_group
    logging.info('Adding cache PV %s to VG %s', cache_pv, vg)
    processes.log_and_call(['vgextend', vg, cache_pv])

    for record in list_cache_records(config):
        configure_caching(lvm.lv_path(vg, record), config)


def disable_cache(config):
    vg = config.volume_group
    for record in list_cache_records(config):
        disable_cache_on(lvm.lv_path(vg, record))

    cache_pv = config.cache_pv
    try:
        logging.info('Removing cache PV %s from VG %s', cache_pv, vg)
        processes.log_and_call(['vgreduce', vg, cache_pv])
    except Exception:
        logging.exception('Failed to remove cache PV from VG')

    try:
        logging.info('Destroying cache PV %s', cache_pv)
        processes.log_and_call(['pvremove', '-f', cache_pv])
    except Exception:
        logging.exception('Failed to destroy cache PV')
