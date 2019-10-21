import contextlib
import datetime
import logging
import os
import tempfile
import time


@contextlib.contextmanager
def temporary_file(**tempfile_args):
    file_ = tempfile.NamedTemporaryFile(delete=False, **tempfile_args)
    try:
        file_.close()
        yield file_.name
    finally:
        os.remove(file_.name)


class Timeout(RuntimeError):
    pass


def wait_for_condition(total, step, check, step_msg, fail_msg):
    start = datetime.datetime.now()
    while datetime.datetime.now() - start < total:
        if check():
            return True
        else:
            logging.info('{}, sleeping for {} seconds'.format(
                step_msg, step.seconds))
            time.sleep(step.seconds)
    raise RuntimeError(fail_msg)


def parse_xl_config(filename):
    config = {}
    with open(filename) as config_file:
        exec(config_file.read(), config)
    return config


def change_name(xl_config, name):
    orig_name = xl_config['name']
    xl_config['name'] = name
    return orig_name


def change_disk(xl_config, index, disk):
    orig_spec = xl_config['disk'][index].split(',')
    new_spec = [disk] + orig_spec[1:]
    xl_config['disk'][index] = ','.join(new_spec)
    return xl_config


def write_xl_config(config, filename):
    with open(filename, 'w') as outfile:
        for key in ['name', 'builder', 'disk', 'memory', 'boot', 'vif', 'cpus',
                    'vcpus', 'localtime', 'vncconsole', 'vnc', 'vnclisten']:
            if key in config:
                value = config[key]
                fmt = '{}="{}"\n' if type(value) is str else '{}={}\n'
                outfile.write(fmt.format(key, config[key]))


def get_hard_disks(xl_config):
    disks = xl_config.get('disk', [])
    hdds = []
    # TODO: this is the parser for the simplest form, man xl-disk-configuration
    # describes a number of another disk specs
    for spec in disks:
        parts = spec.split(',')
        assert len(parts) == 4
        if 'cdrom' not in parts[3]:
            hdds.append(parts[0])
    return hdds
