def get_hostname(host):
    return f'{host.sname}-win'.upper()


def get_possible_logins(host, login):
    return [f'{get_hostname(host)}+{login}', login]
