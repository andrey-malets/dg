import json
import urllib.error
import urllib.parse
import urllib.request


def get(base_url, entity):
    reply = urllib.request.urlopen(f'{base_url}/{entity}')
    result = json.load(reply)
    reply.close()
    return result


def set(base_url, entity, props):
    urllib.request.urlopen(
        f'{base_url}/{entity}',
        urllib.parse.urlencode(props).encode()
    ).close()
