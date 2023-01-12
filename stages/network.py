import contextlib
import subprocess

from common import config, stage


class EnsureNetworkSpeed(config.WithLocalAddress,
                         config.WithSSHCredentials,
                         stage.ParallelStage):
    'ensure sufficient throughput of network interface'

    def __init__(self, poolsize=10, minimum=500, time=5):
        super().__init__(poolsize)
        self.minimum = minimum
        self.time = time
        self.server = None

    @contextlib.contextmanager
    def prepared(self):
        server = subprocess.Popen(
            ['iperf', '-s'], stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )
        try:
            with super().prepared():
                yield
        finally:
            server.terminate()
            server.wait()

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
                return
            speed = int(tokens[8]) / 1000000
            if speed < self.minimum:
                self.fail(
                    ('insufficient network speed: need {} Mbits/s, ' +
                     'got {} Mbits/s').format(self.minimum, speed))
                return
            elif speed < self.minimum * 1.2:
                host.state.log.warning(
                    ('measured network speed for {} is {} Mbits/s, ' +
                     'which is close to minimum of {} Mbits/s').format(
                        host.name, speed, self.minimum))
            else:
                host.state.log.info(
                    'measured network speed for {} is {} Mbits/s'.format(
                        host.name, speed))
