from util import hosts


class Method(object):
    stages = []

    def __init__(self, stages):
        self.stages = (
            self.__class__.stages if stages is None
            else [self.__class__.stages[index] for index in map(int, stages)]
        )

    def parse(self, args):
        for stage in self.stages:
            stage.parse(args)

    def run(self, state):
        for index, stage in enumerate(self.stages):
            state.log.info('running stage "{}"'.format(stage))
            try:
                stage.run(state)
                state.log.info('finished "{}"'.format(stage))
                state.log.info('active hosts after this stage: {}'.format(
                    hosts.format_hosts(state.active_hosts)))
            except (KeyboardInterrupt, Exception) as e:
                state.log.exception(e)
                state.log.error('stage "{}" failed completely'.format(stage))
                for host in sorted(state.active_hosts):
                    host.fail(stage, 'stage completely failed')

            if len(state.failed_hosts) > 0:
                state.log.warning('failed hosts after "{}": {}'.format(
                    stage, hosts.format_hosts(state.failed_hosts)))
                state.log.warning('doing rollback for those')
                for stage in reversed(self.stages[:index + 1]):
                    try:
                        stage.rollback(state)
                    except Exception as e:
                        state.log.exception(e)
                        state.log.error(
                            'rollback of "{}" failed'.format(stage))
                state.all_failed_hosts.update(state.failed_hosts)
                state.failed_hosts.clear()

            if not state.active_hosts:
                state.log.error('all the hosts failed, stopping now')
                return False

        if len(state.all_failed_hosts) > 0:
            state.log.warning('finished. Failed hosts are: ')
            for host in sorted(state.all_failed_hosts,
                               key=lambda host: host.name):
                stage, reason = host.failure
                state.log.warning('{}, stage: {}, reason: {}'.format(
                                  host.name, stage, reason))
        else:
            state.log.info('finished.')
        return True
