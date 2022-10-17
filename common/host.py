class Host(object):
    def __init__(self, state, config):
        self.state = state
        self.name = config['name']
        self.sname = config.get('sname')
        self.props = config.get('props', {})
        self.amt_host = None
        self.disk = None
        self.failure = None
        self.state.active_hosts.add(self)

    def __str__(self):
        return self.name

    def fail(self, stage, reason):
        self.state.log.error(
            f'host {self} failed, stage: {stage}, reason: {reason}'
        )
        self.failure = (stage, reason)
        self.state.active_hosts.remove(self)
        self.state.failed_hosts.add(self)
