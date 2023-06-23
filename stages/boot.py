from common import config, stage
from clients import config as cfg


class ConfigureBoot(config.WithConfigURL, stage.SimpleStage):
    BOOT_PROP = 'boot'

    LOCAL_COW = 'grub.cow'
    COW_MEMORY = 'cow-m'
    DEFAULT = ''

    def set(self, host, value):
        cfg.set(self.config_url, host.name, [(ConfigureBoot.BOOT_PROP, value)])

    def rollback_single(self, host):
        self.set(host, ConfigureBoot.DEFAULT)


def BootsToLocalLinuxByDefault(host):
    return host.props.get('boot') == ConfigureBoot.LOCAL_COW


class SetBootIntoCOWMemory(ConfigureBoot):
    'enable boot to COW memory image'

    def run_single(self, host):
        self.set(host, ConfigureBoot.COW_MEMORY)


class SetBootIntoLocalWindows(ConfigureBoot):
    'enable boot to local Windows'

    def run_single(self, host):
        windows_partition = (
            host.props.get('windows', {}).get('boot_partition', 'windows10')
        )
        self.set(host, f'grub.{windows_partition}')


class SetBootIntoLocalLinux(ConfigureBoot):
    'enable boot to local Linux'

    def run_single(self, host):
        self.set(host, ConfigureBoot.LOCAL_COW)


class ResetBoot(ConfigureBoot):
    "reset boot into it's default state"

    def run_single(self, host):
        self.set(host, ConfigureBoot.DEFAULT)
