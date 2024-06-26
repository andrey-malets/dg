from common import config, stage
from util import win
import os


class RunCommands(stage.ParallelStage):
    def get_files_to_copy(self, host):
        return []

    def get_commands(self, host):
        return []

    def run_single(self, host):
        rvs = [self.run_scp(host, self.ssh_login_linux, src, dst)[0]
               for src, dst in self.get_files_to_copy(host)]
        rvs += [self.run_ssh(host, cmd, login=self.ssh_login_linux)[0]
                for cmd in self.get_commands(host)]

        if any(rvs):
            self.fail('failed to {}'.format(self))


class StoreCOWConfig(config.WithSSHCredentials, RunCommands):
    'store Puppet SSL stuff into COW config partition'

    def get_commands(self, host):
        return list(['/root/cow/conf.sh'] + cmd for cmd in [
            ['mkdir', '-p', '{}/puppet/certs', '{}/puppet/private_keys'],
            ['cp', '-a', '/var/lib/puppet/ssl/certs/ca.pem',
             '{}/puppet/certs'],
            ['cp', '-a', '/var/lib/puppet/ssl/certs/{}.pem'.format(host.name),
             '{}/puppet/certs'],
            ['cp', '-a', '/var/lib/puppet/ssl/private_keys/{}.pem'.format(
                host.name), '{}/puppet/private_keys']
        ])


class CustomizeWindowsSetup(
        config.WithSSHCredentials, config.WithWindowsRootPartition,
        config.WithWindowsDataPartition, config.WithWindowsDriverSearchPath,
        RunCommands):
    'customize SSH credentials and sysprep config in Windows root partition'

    def get_files_to_copy(self, host):
        win7 = os.path.join(os.path.dirname(__file__), os.path.pardir, 'win7')
        files = [(os.path.join(win7, 'customize.py'), '/tmp/customize.py')]

        if self.win_data_label:
            files.append((os.path.join(win7, 'filter_reg.py'),
                          '/tmp/filter_reg.py'))
        return files

    def get_commands(self, host):
        mountpoint = '/mnt'
        is_cygwin = host.props.get('windows', {}).get('is_cygwin', False)
        prefix = ('/cygwin64/etc' if is_cygwin else '/ProgramData/ssh')
        args = ['-H', win.get_hostname(host)]
        if 'userqwer' in host.props['services']:
            args += ['-a', 'user:qwer', '-A', 'user:qwer']
        if self.driver_path:
            args += ['-d', self.driver_path]
        sysprep_xml = '{}/Windows/Panther/unattend.xml'.format(mountpoint)
        if self.win_data_label is not None:
            args += ['-c', r'"{} {} {}:\\"'.format(
                r'C:\\Windows\\Setup\\Scripts\\set-mountpoint.exe',
                self.win_data_label, self.win_data_letter)]
            home = r'{}:\\Users'.format(self.win_data_letter)
            args += ['-c', r'"cmd /c mkdir {}"'.format(home)]
            args += ['-P', home]
        cmds = [
            ['mount', self.win_root_partition, mountpoint],
            ['cp /etc/ssh/ssh_host_*_key{{,.pub}} {}{}'.format(
                mountpoint, prefix)],
            ['python3', '/tmp/customize.py'] + args + [sysprep_xml,
                                                       sysprep_xml],
        ]
        hardware = host.props.get('hardware')
        if hardware:
            setup = '/mnt/drivers/setup.cmd'
            cmds.append([
                'bash -c',
                '"echo \'call %~dp0setup-impl.cmd {}\' > {}"'.format(
                    hardware, setup)
            ])
        if self.win_data_label:
            cmds.append([
                'sed', '-i',
                '"s/rem set profiles=/set profiles=' +
                r'{}:\\\\Users\\\\profiles.reg/"'.format(self.win_data_letter),
                '{}/Windows/Setup/Scripts/SetupComplete.cmd'.format(mountpoint)
            ])
        cmds.append(['umount', mountpoint])

        if self.win_data_label:
            cmds.append(['mount', self.get_win_data_partition(), mountpoint])
            cmds.append(['rm', '-rf', '{}/Users/Administrator*'.format(
                mountpoint)])
            cmds.append(['rm', '-rf', '{}/Users/UpdatusUser*'.format(
                mountpoint)])
            cmds.append(['python3', '/tmp/filter_reg.py', '-q',
                         '-f', '".+-500$"',
                         '{}/Users/profiles.reg'.format(mountpoint),
                         '{}/Users/profiles.reg'.format(mountpoint)])
            cmds.append(['umount', mountpoint])
        return cmds
