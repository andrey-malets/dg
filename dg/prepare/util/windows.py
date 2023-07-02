import contextlib
import logging
import subprocess
import tempfile

from dg.prepare.util import processes


LOGIN = 'Administrator'
SSH_OPTIONS = ('-l', LOGIN, '-o', 'ConnectTimeout=1')
SCP_OPTIONS = ('-o', f'User={LOGIN}', '-o', 'ConnectTimeout=1')


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


def scp(host, src, dst, back=False):
    logging.info('Copying %s to %s on %s', src, dst, host)
    return processes.scp(host, src, dst, options=SCP_OPTIONS, back=back)


@contextlib.contextmanager
def collected_soft(host, cygwin, output):
    collect_cmd = f'wmic /output:{output} product get name,version'
    try:
        logging.debug('Collecting installed software on %s to %s: %s',
                      host, output, collect_cmd)
        ssh(host, collect_cmd)
        yield output
    finally:
        del_cmd = f'{"rm" if cygwin else "del"} {output}'
        try:
            logging.debug('Removing collected software file %s on %s',
                          output, host)
            ssh(host, del_cmd)
        except Exception:
            logging.exception('Failed to remove %s on %s', output, host)


def collect_installed_packages(host, cygwin):
    with contextlib.ExitStack() as stack:
        output = stack.enter_context(
            tempfile.NamedTemporaryFile(prefix=f'{host}_soft_', suffix='.txt')
        )
        host_output = stack.enter_context(
            collected_soft(host, cygwin, 'soft.txt')
        )
        scp(host, host_output, output.name, back=True)
        return output.read().decode('utf-16')
