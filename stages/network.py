import os
import subprocess

from common import config, stage
from util import proc

class EnsureNetworkSpeed(config.WithLocalAddress,
                         config.WithSSHCredentials,
                         stage.ParallelStage):
    'ensure sufficient throughput of network interface'

    def __init__(self, poolsize=3, minimum=150, time=5):
        stage.ParallelStage.__init__(self, poolsize)
        self.minimum = minimum
        self.time = time
        self.server = None

    def setup(self):
        self.output = open(os.devnull, 'w')
        self.server = subprocess.Popen(['iperf', '-s'],
                                       stdout = self.output,
                                       stderr = subprocess.STDOUT)

    def run_single(self, host):
        rv, output = self.run_ssh(
            host,
            ['iperf', '-c', self.local_addr, '-t', str(self.time), '-y', 'c'],
            login=self.ssh_login_linux)
        if rv != 0:
            self.fail('failed to execute iperf -c, rv is {}'.format(rv))
        else:
            tokens = output.strip().split(',')
            if len(tokens) != 9:
                self.fail(
                    'failed to parse iperf output, it was: {}'.format(output))
            speed = int(tokens[8]) / 1000000
            if speed < self.minimum:
                self.fail(
                    ('insufficient network speed: need {} Mbits/s, ' +
                     'got {} Mbits/s').format(self.minimum, speed))
            elif speed < self.minimum * 1.2:
                host.state.log.warning(
                    ('measured network speed for {} is {} Mbits/s, ' +
                     'which is close to minimum of {} Mbits/s').format(
                        host.name, speed, self.minimum))
            else:
                host.state.log.info(
                    'measured network speed for {} is {} Mbits/s'.format(
                        host.name, speed))

    def teardown(self):
        self.output.close()
        self.server.terminate()
        self.server = None
