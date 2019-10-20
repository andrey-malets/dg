import datetime
import logging
import subprocess

from dg.win.prepare import util


class SSHClient(object):
    def __init__(self, host, login):
        self.host = host
        self.login = login

    def ssh(self, *cmd):
        cmdline = ['ssh', '-o', 'ConnectTimeout=3',
                   '-l', self.login, self.host] + list(cmd)
        logging.info('running {}'.format(cmdline))
        return subprocess.check_output(cmdline)

    def scp(self, src, dest):
        cmdline = ['scp', '-o', 'ConnectTimeout=5',
                   src, '{}@{}:{}'.format(self.login, self.host, dest)]
        logging.info('running {}'.format(cmdline))
        subprocess.check_call(cmdline)

    def is_ready(self):
        try:
            self.ssh('exit')
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
