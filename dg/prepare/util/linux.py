import logging
import subprocess

from dg.prepare.util import processes
from dg.prepare.util import wait


def no_dpkg_locks(host):
    return processes.ssh(host, '! fuser /var/lib/dpkg/lock') == 0


def shutdown(host):
    logging.info('Waiting for no dpkg locks on %s', host)
    wait.wait_for(lambda: no_dpkg_locks(host), timeout=900, step=10)
    logging.info('Shutting down %s', host)
    processes.ssh(host, 'shutdown now')


def reboot(host):
    logging.info('Rebooting %s', host)
    processes.ssh(host, 'reboot')


def is_accessible(host):
    logging.info('Checking if %s is accessible', host)
    return processes.ssh(host, 'id', options=('-o', 'ConnectTimeout=1'),
                         stdout=subprocess.PIPE) == 0


def try_reboot_if_idle(host):
    logging.info('Checking if host %s is idle', host)
    try:
        who = processes.ssh(host, 'who', output=True,
                            options=('-o', 'ConnectTimeout=1')).strip()
    except Exception:
        logging.exception('Failed to check if host %s is idle', host)
        return

    if who:
        logging.info('Host %s is busy, skipping reboot', host)
    else:
        try:
            reboot(host)
        except Exception:
            logging.exception('Failed to reboot host %s', host)
