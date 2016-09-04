def get_hostname(host):
    return ('{}-win'.format(host.sname)).upper()


def get_possible_logins(host, login):
    return ['{}+{}'.format(get_hostname(host), login), login]
