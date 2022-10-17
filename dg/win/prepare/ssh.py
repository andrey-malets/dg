import datetime
import logging
import subprocess

import psutil

from dg.win.prepare import util


class SSHClient(object):
    def __init__(self, host, login):
        self.host = host
        self.login = login

    def ssh(self, cmd, timeout=None):
        cmdline = ['ssh', '-o', 'ConnectTimeout=3',
                   '-l', self.login, self.host] + cmd
        logging.info('running {}'.format(cmdline))
        proc = psutil.Popen(cmdline, stdout=subprocess.PIPE, text=True)
        try:
            proc.wait(timeout.total_seconds() if timeout else None)
        except psutil.TimeoutExpired:
            try:
                proc.kill()
                proc.wait()
            except psutil.NoSuchProcess:
                pass
            raise

        output = proc.stdout.read()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd, output)
        return output

    def scp(self, src, dest):
        cmdline = ['scp', '-o', 'ConnectTimeout=5',
                   src, '{}@{}:{}'.format(self.login, self.host, dest)]
        logging.info('running {}'.format(cmdline))
        subprocess.check_call(cmdline)

    def is_ready(self):
        try:
            self.ssh(['exit'])
            return True
        except subprocess.CalledProcessError:
            return False

    def wait_ssh_ready(self, timeout):
        logging.info('waiting for ssh to come up on {}'.format(self.host))
        return util.wait_for_condition(
            total=timeout, step=datetime.timedelta(seconds=3),
            check=lambda: self.is_ready(),
            step_msg='{}@{} is not accessible yet'.format(
                self.login, self.host),
            fail_msg=('timed out while waiting for {}@{} '
                      'to become available'.format(self.login, self.host)))
