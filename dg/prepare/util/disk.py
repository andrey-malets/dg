import contextlib
import logging
import os
import subprocess
import time

from dataclasses import dataclass

from dg.prepare.util import processes
from dg.prepare.util import transactions


@dataclass(frozen=True)
class DiskConfiguration:
    path: str
    size: str
    transport: str
    logical_sector_size: int
    physical_sector_size: int
    partition_table_type: str
    model: str


@dataclass(frozen=True)
class PartitionConfiguration:
    number: int
    begin: str
    end: str
    size: str
    filesystem_type: str
    name: str
    kpartx_name: str
    flags_set: str


@dataclass(frozen=True)
class DiskInformation:
    type: str
    configuration: DiskConfiguration
    partitions: list


class DiskConfigError(Exception):

    def __init__(self, message, device, real_device, parted_output):
        super().__init__(
            f'{message} for device {device} (real device {real_device}). '
            f'Parted output was: {parted_output}'
        )


def cleanup_kpartx(device):
    cmdline = ['kpartx', '-d', '-v', device]
    for delay in (0.1, 0.3, 0.5, 1, 2, 3, None):
        result = processes.log_and_call(
            cmdline, method=subprocess.run, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        if result.returncode == 0:
            return
        if 'is in use' in result.stdout:
            logging.warning('Some partitions of %s are still in use: ', device)
            logging.warning(result.stdout)
            if delay is not None:
                logging.info('waiting for %.01f seconds', delay)
                time.sleep(delay)
        else:
            raise RuntimeError('Unexpected error from kpartx: '
                               f'{result.stdout}')

    raise RuntimeError(f'Failed to cleanup partitions for {device} '
                       'with kpartx')


def get_kpartx_names(device):
    cmdline = ['kpartx', '-l', '-s', device]
    logging.debug('Running %s', cmdline)
    try:
        output = processes.log_and_output(cmdline)
        result = {}
        for index, line in enumerate(output.splitlines()):
            name = line.split(' ', 1)[0]
            result[int(index + 1)] = f'/dev/mapper/{name}'
        return result
    finally:
        try:
            cleanup_kpartx(device)
        except Exception:
            logging.exception('Exception while cleaning up partitions '
                              'for device %s', device)


@contextlib.contextmanager
def partitions_exposed(device):
    with transactions.transact(
        prepare=(
            f'Exposing kpartx partitions for {device}',
            lambda: processes.log_and_call(['kpartx', '-a', '-s', device])
        ),
        final=(
            f'cleaning up partitions for device {device}',
            lambda _: cleanup_kpartx(device)
        )
    ):
        yield


def parse_partitions(device, lines):
    kpartx_names = get_kpartx_names(device)
    for line in lines:
        assert line.endswith(';')
        number, begin, end, size, fs, name, flags = line[:-1].split(':')
        yield PartitionConfiguration(
            number=int(number),
            begin=begin,
            end=end,
            size=size,
            filesystem_type=fs,
            name=name,
            kpartx_name=kpartx_names[int(number)],
            flags_set=flags,
        )


def get_disk_information(device):
    real_device = os.path.realpath(device)
    output = processes.log_and_output([
        'parted', '-s', '-m', real_device, 'print'
    ])
    lines = list(line.strip() for line in output.splitlines())
    if len(lines) < 2:
        raise DiskConfigError(
            'Expected at least two lines in parted output',
            device, real_device, output
        )

    BYTES = 'BYT'
    if lines[0] != f'{BYTES};':
        raise DiskConfigError(
            'Only "Bytes" units are supported',
            device, real_device, output
        )

    path, size, transport, lss, pss, ptt, model, end = lines[1].split(':')
    if path != real_device:
        raise DiskConfigError(
            'Expected device spec as second line of parted output',
            device, real_device, output
        )

    disk_config = DiskConfiguration(
        path=path,
        size=size,
        transport=transport,
        logical_sector_size=int(lss),
        physical_sector_size=int(pss),
        partition_table_type=ptt,
        model=model,
    )

    return DiskInformation(
        type=BYTES,
        configuration=disk_config,
        partitions=list(parse_partitions(device, lines[2:])),
    )


def get_partition(device, disk_info, name):
    parts = list(part for part in disk_info.partitions if part.name == name)
    if len(parts) != 1:
        raise RuntimeError(f'Expected exactly one partition with name {name} '
                           f'on device {device}, got {disk_info.partitions}')
    return parts[0]


def set_partition_name(device, number, name):
    logging.info('Setting partition name to %s for partition number %d on %s',
                 name, number, device)
    processes.log_and_call(['parted', '-s', device, 'name', str(number), name])
