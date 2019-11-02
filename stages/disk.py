from common import config, stage


class ConfigureDisk(config.WithSSHCredentials, config.WithConfigURL,
                    stage.ParallelStage):
    'call disk.py to configure state of local disk'

    def run_single(self, host):
        rv, _ = self.run_ssh(host, ['disk.py', '-c', self.config_url],
                             login=self.ssh_login_linux)
        if rv != 0:
            self.fail('call to disk.py failed')


class FreeDisk(config.WithSSHCredentials, stage.ParallelStage):
    'possibly unmount /place to free local disk'

    def run_single(self, host):
        host.state.log.info('Stopping Docker which might hold files in /place')
        self.run_ssh(host, ['systemctl', 'stop', 'docker'],
                     login=self.ssh_login_linux)

        rv, _ = self.run_ssh(
            host, ['if mountpoint /place; then umount /place; fi'],
            login=self.ssh_login_linux)
        if rv != 0:
            self.fail('failed to free local disk')
