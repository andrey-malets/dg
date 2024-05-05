import json

from common import config, stage


class ConfigureDisk(config.WithSSHCredentials, config.WithConfigURL,
                    stage.ParallelStage):
    'call disk.py to configure state of local disk'

    def run_single(self, host):
        self.run_ssh_checked(host, ['disk.py', '-c', self.config_url],
                             login=self.ssh_login_linux, description=self)


class FreeDisk(config.WithSSHCredentials, stage.ParallelStage):
    'stop processes using local disk to prepare the disk for partitioning'

    POSSIBLE_MOUNTPOINTS = ['/place']

    def run_single(self, host):
        try:
            host.state.log.info(
                'stopping Docker which might hold files in /place'
            )
            self.run_ssh_checked(
                host, ['systemctl', 'stop', 'docker.socket', 'docker'],
                login=self.ssh_login_linux, description='stop docker'
            )

            for mp in self.POSSIBLE_MOUNTPOINTS:
                host.state.log.info(f'unmouting {mp} if it is mounted')
                self.run_ssh_checked(
                    host, [f'if mountpoint {mp}; then umount {mp}; fi'],
                    login=self.ssh_login_linux,
                    description=f'unmount {mp} if it is mounted'
                )

            host.state.log.info('deactivating all the LVM volume groups')
            vgs_output = self.run_ssh_checked(
                host, ['vgs', '-o', 'name', '--reportformat', 'json'],
                login=self.ssh_login_linux,
                description='list LVM volume groups'
            )

            vgs = [
                vg['vg_name']
                for vg in json.loads(vgs_output)['report'][0]['vg']
            ]
            self.run_ssh_checked(
                host, ['vgchange', '-a', 'n', *vgs],
                login=self.ssh_login_linux,
                description='deactivate LVM volume groups'
            )
        except Exception as e:
            host.state.log.exception('failed to %s', self)
            self.fail(e)
