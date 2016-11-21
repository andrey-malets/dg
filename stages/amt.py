import subprocess
import os.path

from clients import config as cfg
from common import config, stage


class DetermineAMTHosts(config.WithConfigURL, stage.SimpleStage):
    'determine AMT hosts'

    def run_single(self, host):
        amt_host = host.props.get('amt')
        if amt_host is None:
            host.fail(self, 'host props do not have "amt" attribute')
        else:
            host.amt_host = cfg.get(self.config_url, amt_host)['name']


class AMTStage(config.WithAMTCredentials, stage.SimpleStage):
    def call_amttool(self, host, cmd, special=None):
        AMTTOOL = os.path.join(os.path.dirname(__file__), os.path.pardir,
                               'clients', 'amttool')
        user, passwd = self.amt_creds.get_credentials(host)
        cmdline = ['/usr/bin/perl', AMTTOOL, host, cmd]
        if special:
            cmdline.append(special)
        return subprocess.check_output(
            cmdline,
            env={'AMT_USER': user, 'AMT_PASSWORD': passwd})


class WakeupAMTHosts(AMTStage):
    'wake up hosts via AMT interface'

    def run_single(self, host):
        try:
            status = self.call_amttool(host.amt_host, 'powerstate')
            if status != 0:
                self.call_amttool(host.amt_host, 'powerup')
        except Exception:
            host.fail(self, 'call to amttool failed')


class ResetAMTHosts(AMTStage):
    'reset hosts via AMT interface and boot to PXE'

    def run_single(self, host):
        try:
            self.call_amttool(host.amt_host, 'reset', 'pxe')
        except Exception:
            host.fail(self, 'call to amttool failed')
