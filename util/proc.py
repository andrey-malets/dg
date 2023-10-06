import subprocess


def run_process(args, log, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    log.info(f'running {args}')
    proc = subprocess.Popen(
        args, stdout=stdout, stderr=stderr, text=True
    )
    stdout, stderr_ = proc.communicate()

    if stderr_:
        for line in stderr_.splitlines():
            log.info('stderr: %s', line)

    return (proc.returncode, stdout)


def run_remote_process(host, login, args, log, opts):
    cmdline = ['ssh', '-l', login,
               '-o', 'PasswordAuthentication=no', '-o', 'BatchMode=yes']
    if opts is not None:
        for opt in opts:
            cmdline.extend(['-o', opt])
    cmdline.append(host)
    cmdline.extend(args)

    return run_process(cmdline, log)
