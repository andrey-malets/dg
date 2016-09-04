import json
import urllib
import urllib2


class AmtredirdError(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


def _do(base_url, cmd, data=None):
    reply = urllib2.urlopen("{}/{}".format(base_url, cmd), data)
    result = json.load(reply)
    reply.close()
    if 'error' in result:
        raise AmtredirdError(result['error'])
    return result


def _list(clients):
    return urllib.urlencode([(client, client) for client in clients])


def _post(base_url, cmd, clients):
    result = _do(base_url, cmd, _list(clients))
    assert len(result) == len(clients)
    return result


def list(base_url):
    result = _do(base_url, 'list')
    assert len(result) == 2 and result[0] == 0
    return result[1]


def start(base_url, clients):
    return _post(base_url, 'start', clients)


def stop(base_url, clients):
    return _post(base_url, 'stop', clients)
