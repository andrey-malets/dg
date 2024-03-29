import logging
import subprocess


def log_and_call(cmdline, method=subprocess.check_call, **kwargs):
    logging.debug('Running %s', cmdline)
    return method(cmdline, **kwargs)


def log_and_output(cmdline, **kwargs):
    return log_and_call(cmdline, method=subprocess.check_output, text=True,
                        **kwargs)


def ssh(host, command, output=False, options=None, method=subprocess.call,
        **kwargs):
    cmdline = ['ssh']
    if options is not None:
        cmdline.extend(options)
    cmdline.extend([host, command])
    if output:
        return log_and_output(cmdline, **kwargs)
    else:
        return log_and_call(cmdline, method=method, **kwargs)


def scp(host, src, dst, options=None, back=False, **kwargs):
    cmdline = ['scp']
    if options is not None:
        cmdline.extend(options)
    cmdline.extend([f'{host}:{src}', dst] if back else [src, f'{host}:{dst}'])
    return log_and_call(cmdline, method=subprocess.check_call, **kwargs)
