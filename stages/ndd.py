import contextlib
import os
import subprocess

from clients import config as cfg
from common import config, stage
from util import proc


@contextlib.contextmanager
def exposed_partitions(path, log):
    path = (
        os.path.join(os.path.dirname(path), os.readlink(path))
        if os.path.islink(path) else path
    )

    list_output = subprocess.check_output(['kpartx', '-l', path], text=True)
    partitions = [
        '/dev/mapper/{}'.format(line.split()[0])
        for line in list_output.splitlines()
    ]

    log.info('Exposing partitions of %s with kpartx', path)
    subprocess.check_call(['kpartx', '-a', '-r', path])
    try:
        yield partitions
    finally:
        log.info('Un-exposing partitions of %s', path)
        subprocess.call(['kpartx', '-d', path])


class RunNDD(config.WithLocalAddress, config.WithNDDArgs, config.WithConfigURL,
             config.WithSSHCredentials, stage.Stage):
    'deploy the images with ndd'

    @classmethod
    @contextlib.contextmanager
    def prepared_input(cls, input_, iargs, log):
        input_partition = None
        for opt in iargs:
            if opt.startswith('p'):
                input_partition = int(opt[1:])

        if input_partition is not None:
            with exposed_partitions(input_, log) as parts:
                yield parts[input_partition - 1]
        else:
            yield input_

    def run(self, state):
        for spec in self.ndds:
            with self.prepared_input(spec.input_, spec.iargs, state.log) \
                    as input_:
                cmdline = [
                    '/usr/local/bin/ndd.py', '-p', self.ndd_port,
                    '-i', input_, '-o', spec.output
                ]

                if spec.source:
                    remote_source = cfg.get(
                        self.config_url, spec.source)['name']
                    source = '{}@{}'.format(self.get_login(), remote_source)
                else:
                    cmdline.extend(['--local'])
                    remote_source = None
                    source = self.local_addr
                cmdline.extend(['-s', source])

                if spec.args:
                    cmdline.extend(['-{}'.format(spec.args)])

                for host in sorted(state.active_hosts,
                                   key=lambda host: host.props.get('switch')):
                    if remote_source and host.name == remote_source:
                        continue
                    cmdline.extend(
                        ['-d', '{}@{}'.format(self.get_login(),
                                              str(host.name))])

                rv, _ = proc.run_process(cmdline, state.log)
                if rv != 0:
                    for host in list(state.active_hosts):
                        host.fail(self, 'failed to run ndd.py')
