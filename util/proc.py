import subprocess


def run_process(args, log):
    log.info(f'running {args}')
    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    stdout, stderr = proc.communicate()

    if len(stderr) > 0:
        log.error(stderr)

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
