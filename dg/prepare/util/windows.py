import contextlib
import logging
import subprocess

from dg.prepare.util import processes


SSH_OPTIONS = ('-l', 'Administrator', '-o', 'ConnectTimeout=1')
SCP_OPTIONS = ('-o', 'User=Administrator', '-o', 'ConnectTimeout=1')


def is_accessible(host):
    logging.info('Checking if %s is accessible', host)
    return processes.ssh(
        host, 'whoami', options=SSH_OPTIONS, stdout=subprocess.PIPE
    ) == 0


def ssh(host, command, **kwargs):
    return processes.ssh(host, command, options=SSH_OPTIONS, **kwargs)


@contextlib.contextmanager
def ssh_away(host, command):
    cmdline = ['ssh']
    cmdline.extend(SSH_OPTIONS)
    cmdline.extend((host, command))
    logging.debug('Starting "away" command %s', cmdline)
    proc = subprocess.Popen(cmdline)
    try:
        yield proc
    finally:
        try:
            logging.debug('Stopping and waiting for "away" command %s',
                          cmdline)
            proc.kill()
            proc.wait()
        except Exception:
            logging.exception('Failed to stop and wait for "away" command %s',
                              cmdline)


def shutdown(host):
    logging.info('Shutting down %s', host)
    return ssh(host, 'shutdown /s /t 0')


def scp(host, src, dst):
    logging.info('Copying %s to %s on %s', src, dst, host)
    return processes.scp(host, src, dst, options=SCP_OPTIONS)
