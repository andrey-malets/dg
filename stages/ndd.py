from clients import config as cfg
from common import config, stage
from util import proc

class RunNDDViaSlurm(config.WithLocalAddress, config.WithNDDArgs, stage.Stage):
    'deploy the images with ndd via SLURM'

    def run(self, state):
        for src, dst in self.ndds:
            cmdline = ['python', '/usr/local/bin/ndd_slurm.py']
            cmdline.extend(['-s', self.local_addr, '-i', src, '-o', dst])

            for host in sorted(state.active_hosts,
                               key=lambda host: host.props.get('switch')):
                cmdline.extend(['-d', str(host.sname)])

            rv, _ = proc.run_process(cmdline, state.log)
            if rv != 0:
                for host in list(state.active_hosts):
                    host.fail(self, 'failed to run ndd_slurm.py')


class RunNDD(config.WithLocalAddress, config.WithNDDArgs, config.WithConfigURL,
             config.WithSSHCredentials, stage.Stage):
    'deploy the images with ndd'

    def run(self, state):
        for spec in self.ndds:
            cmdline = ['python', '/usr/local/bin/ndd_slurm.py', '-H',
                       '-i', spec.input_, '-o', spec.output]
            if spec.source:
                source = cfg.get(self.config_url, spec.source)['name']
                cmdline.extend(['-s', source])
            else:
                source = None
                cmdline.extend(['-s', self.local_addr])

            if spec.args:
                cmdline.extend(['-{}'.format(spec.args)])

            for host in sorted(state.active_hosts,
                               key=lambda host: host.props.get('switch')):
                if source and host.name == source:
                    continue
                cmdline.extend(
                    ['-d', '{}@{}'.format(self.get_login(), str(host.sname))])

            if source:
                rv, _ = proc.run_remote_process(
                    source, self.ssh_login_linux, cmdline, state.log, None)
            else:
                rv, _ = proc.run_process(cmdline, state.log)

            if rv != 0:
                for host in list(state.active_hosts):
                    host.fail(self, 'failed to run ndd_slurm.py')
