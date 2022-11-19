from common import config, stage
from clients import config as cfg


def BootsToWindowsByDefault(host):
    return host.props.get('boot') == ConfigureBoot.WINDOWS


class ConfigureBoot(config.WithConfigURL, stage.SimpleStage):
    BOOT_PROP = 'boot'

    LOCAL_COW = 'grub.cow'
    COW_MEMORY = 'cow-m'
    WINDOWS = 'grub.windows7'
    DEFAULT = ''

    def set(self, host, value):
        cfg.set(self.config_url, host.name, [(ConfigureBoot.BOOT_PROP, value)])

    def rollback_single(self, host):
        self.set(host, ConfigureBoot.DEFAULT)


class SetBootIntoCOWMemory(ConfigureBoot):
    'enable boot to COW memory image'

    def run_single(self, host):
        self.set(host, ConfigureBoot.COW_MEMORY)


class SetBootIntoLocalWindows(ConfigureBoot):
    'enable boot to local Windows'

    def run_single(self, host):
        self.set(host, ConfigureBoot.WINDOWS)


class SetBootIntoLocalLinux(ConfigureBoot):
    'enable boot to local Linux'

    def run_single(self, host):
        self.set(host, ConfigureBoot.LOCAL_COW)


class ResetBoot(ConfigureBoot):
    "reset boot into it's default state"

    def run_single(self, host):
        self.set(host, ConfigureBoot.DEFAULT)
