import HTMLParser
import time
import re
import requests

from common import config, stage

class TParser(HTMLParser.HTMLParser):

    def handle_starttag(self, tag, attrs):
        attrs_map = dict(attrs)
        if tag == 'input' and attrs_map.get('name') == 't':
            self.t = attrs_map.get('value')


class StdMStage(config.WithAMTCredentials, stage.ParallelStage):
    ON_RE = re.compile('Power state: On')
    OFF_RE = re.compile('Power state: Off')

    def make_request(self, method, host, url, validate=True, **args):
        response = method('http://{}:16992/{}'.format(host, url),
                          auth=requests.auth.HTTPDigestAuth(
                              *self.amt_creds.get_credentials(host)),
                          **args)
        if validate:
            response.raise_for_status()
        return response

    def get(self, host, url):
        return self.make_request(requests.get, host, url)

    def post(self, host, url, **params):
        return self.make_request(requests.post, host, url, validate=False,
                                 data=params)

    def boot_control(self, host, **params):
        parser = TParser()
        parser.feed(self.get(host, 'remote.htm').text)
        return self.post(host, 'remoteform', t=parser.t, **params)


class WakeupStdMHosts(StdMStage):
    'wake up hosts via Std. Manageability interface'

    def get_status(self, host, log):
        try:
            response_text = self.get(host, 'remote.htm').text
            on = WakeupStdMHosts.ON_RE.search(response_text) != None
            off = WakeupStdMHosts.OFF_RE.search(response_text) != None
            assert (on and not off) or (off and not on), \
                'Host status check failed: on={} off={}'.format(on, off)
            return True, on
        except Exception as e:
            log.exception("Failed to determine host status")
            return False, False

    def is_up(self, host, timeouts, log):
        for timeout in timeouts:
            time.sleep(timeout)
            log.info('Checking if {} is up'.format(host))
            known, is_up = self.get_status(host, log)
            if known:
                return is_up
        raise RuntimeError('Failed to check whether {} is up'.format(host))

    def wait_until_gets_up(self, host, timeouts, log):
        for timeout in timeouts:
            time.sleep(timeout)
            log.info('Checking if {} got up'.format(host))
            known, is_up = self.get_status(host, log)
            if known and is_up:
                return
        raise RuntimeError('Failed to wait until {} gets up'.format(host))

    def run_single(self, host):
        if not self.is_up(host.amt_host, [0, 3, 5], host.state.log):
            host.state.log.info('Waking up {}'.format(host.amt_host))
            self.boot_control(
                host.amt_host,
                amt_html_rc_radio_group=2, amt_html_rc_boot_special=1)
            self.wait_until_gets_up(host.amt_host, [5, 10, 10, 15],
                                    host.state.log)


class ResetStdMHosts(StdMStage):
    'reset hosts via Std. Manageability interface'

    def run_single(self, host):
        self.boot_control(
            host.amt_host,
            amt_html_rc_radio_group=4, amt_html_rc_boot_special=1)
