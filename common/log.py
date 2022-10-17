import contextlib
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import getpass
import logging
import os
import smtplib
import socket
import sys
import tempfile
import termcolor


class CustomFormatter(logging.Formatter):
    def __init__(self, colored):
        super(CustomFormatter, self).__init__(
            '%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - ' +
            '%(message)s')
        self.colored = colored

    @staticmethod
    def get_color(record):
        return {
            logging.INFO:    'white',
            logging.WARNING: 'yellow',
            logging.ERROR:   'red',
        }.get(record.levelno)

    def format(self, record):
        base = super(CustomFormatter, self).format(record)
        if self.colored:
            return termcolor.colored(base, CustomFormatter.get_color(record))
        else:
            return base


def add_params(parser):
    parser.add_argument(
        '-C', help='Colored log output', action='store_true')
    parser.add_argument(
        '-r', metavar='ADDRESS', action='append',
        help='address(es) to send e-mail with report to')


def send_report(args, state, log_file, start, finish):
    msg = MIMEMultipart()

    dest = ', '.join(sorted(state.groups) + sorted(state.hosts))
    subject = f'Deployment of "{dest}" with "{args.m}" method finished'
    text = 'Command line: {}.\n'.format(' '.join(sys.argv))
    text += '\nStart: {}.\nFinish: {}.\n'.format(
        start.strftime('%c'), finish.strftime('%c'))
    if not state.active_hosts:
        subject += ' (ALL failed)'
    elif state.all_failed_hosts:
        subject += ' ({} failed)'.format(
            ', '.join(map(lambda host: host.sname, state.all_failed_hosts)))

        text += '\n'
        for host in state.all_failed_hosts:
            stage, reason = host.failure
            text += '{} failed, stage: {}, reason: {}\n'.format(
                host.name, stage, reason)

    text += '\nSee the attached log for details.'

    from_ = '{}@{}'.format(getpass.getuser(), socket.getfqdn())

    msg['Subject'] = subject
    msg['From'] = from_
    msg['To'] = '; '.join(args.r)
    msg.attach(MIMEText(text))

    with open(log_file) as log_input:
        log_attach = MIMEText(log_input.read())
        log_attach.add_header('Content-Disposition', 'attachment',
                              filename='log.txt')
        msg.attach(log_attach)

    sender = smtplib.SMTP('localhost')
    sender.sendmail(from_, args.r, msg.as_string())
    sender.quit()


@contextlib.contextmanager
def capturing(args, state):
    log_file = None
    if args.r:
        fd, log_file = tempfile.mkstemp(prefix='dg_{}_'.format(args.m))
        os.close(fd)
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler()

    handler.setFormatter(CustomFormatter(args.C))
    state.log.setLevel(logging.INFO)
    state.log.addHandler(handler)

    start = datetime.datetime.now()
    yield
    finish = datetime.datetime.now()

    state.log.removeHandler(handler)
    handler.close()
    if log_file:
        send_report(args, state, log_file, start, finish)
        os.unlink(log_file)
