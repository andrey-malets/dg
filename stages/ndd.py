from clients import config as cfg
from common import config, stage
from util import proc


class RunNDD(config.WithLocalAddress, config.WithNDDArgs, config.WithConfigURL,
             config.WithSSHCredentials, stage.Stage):
    'deploy the images with ndd'

    def run(self, state):
        for spec in self.ndds:
            cmdline = ['python', '/usr/local/bin/ndd.py', '-p', self.ndd_port,
                       '-i', spec.input_, '-o', spec.output]
            if spec.source:
                remote_source = cfg.get(self.config_url, spec.source)['name']
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
                    ['-d', '{}@{}'.format(self.get_login(), str(host.name))])

            rv, _ = proc.run_process(cmdline, state.log)
            if rv != 0:
                for host in list(state.active_hosts):
                    host.fail(self, 'failed to run ndd.py')
