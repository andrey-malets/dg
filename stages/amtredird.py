from clients import amtredird
from common import config, stage


class EnsureRedirectionPossible(config.WithAMTRedirdURL, stage.Stage):
    'ensure amtrerid has the hosts required'

    def run(self, state):
        possible_hosts = set(amtredird.list(self.amtredird_url))
        for host in sorted(state.active_hosts):
            assert host.amt_host
            if host.amt_host not in possible_hosts:
                host.fail(self, 'AMT host not configured in amtredird')


class ChangeRedirection(config.WithAMTRedirdURL, stage.Stage):
    def run(self, state):
        amt_to_host = {}
        for host in sorted(state.active_hosts):
            assert host.amt_host
            amt_to_host[host.amt_host] = host
        hosts = list(amt_to_host.keys())
        for command in self.commands():
            results = command(self.amtredird_url, hosts)
            for amt_host, (result, args) in results.items():
                if result != 0:
                    amt_to_host[amt_host].fail(self,
                                               'failed to change redirection')


class EnableRedirection(ChangeRedirection):
    'enable IDE-R redirection via amtredird'

    def commands(self):
        return [amtredird.stop, amtredird.start]

    def rollback(self, state):
        amt_to_host = {}
        for host in state.failed_hosts:
            assert host.amt_host
            amt_to_host[host.amt_host] = host
        results = amtredird.stop(self.amtredird_url, list(amt_to_host.keys()))
        for amt_host, (result, args) in results.items():
            if result != 0:
                state.log.warning('failed to stop redirection for {}'.format(
                    amt_to_host[amt_host]))


class DisableRedirection(ChangeRedirection):
    'disable IDE-R redirection via amtredird'

    def commands(self):
        return [amtredird.stop]
