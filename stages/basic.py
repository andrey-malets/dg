from clients import config as cfg
from common import config, host, stage


class InitHosts(config.WithConfigURL, stage.Stage):
    'get initial host list'

    def run(self, state):
        all_hosts = set(cfg.get(self.config_url, sname)['name']
                        for sname in state.hosts)

        for group in state.groups:
            all_hosts |= set(cfg.get(self.config_url, group)['hosts'])

        for name in all_hosts:
            host.Host(state, cfg.get(self.config_url, name))


class ExcludeBannedHosts(config.WithBannedHosts, stage.Stage):
    'exclude banned hosts from deployment'

    def run(self, state):
        for host_ in list(state.active_hosts):
            if any(name in self.banned_hosts
                   for name in (host_.name, host_.sname)):
                host_.fail(self, 'explicitly excluded from deployment')
