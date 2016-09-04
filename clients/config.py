import json
import urllib
import urllib2


def get(base_url, entity):
    reply = urllib2.urlopen('{}/{}'.format(base_url, entity))
    result = json.load(reply)
    reply.close()
    return result


def set(base_url, entity, props):
    urllib2.urlopen('{}/{}'.format(base_url, entity),
                    urllib.urlencode(props)).close()
