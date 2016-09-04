import subprocess


def run_process(args, log):
    log.info('running {}'.format(args))
    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    if len(stderr) > 0:
        log.error(stderr)

    return (proc.returncode, stdout)


def run_remote_process(host, login, args, log, opts):
    cmdline = ['ssh', '-l', login, '-o', 'PasswordAuthentication=no']
    if opts is not None:
        for opt in opts:
            cmdline.extend(['-o', opt])
    cmdline.append(host)
    cmdline.extend(args)

    return run_process(cmdline, log)
