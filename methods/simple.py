import datetime

from common import method
from stages import basic, boot, config, ndd, network, slurm, ssh

class SimpleMethod(method.Method):
    'method for deploying pre-configured machines'
    name = 'simple'

    stages = [
        basic.InitHosts(),
        basic.ExcludeBannedHosts(),
        ssh.CheckIsAccessibleViaSSH(*ssh.Timeouts.TINY),
        boot.SetBootIntoCOWMemory(),
        ssh.RebootHost(*ssh.Timeouts.TINY),
        ssh.WaitUntilBootedIntoCOWMemory(*ssh.Timeouts.NORMAL),
        boot.ResetBoot(),
        config.StoreCOWConfig(),
        network.EnsureNetworkSpeed(),
        slurm.WaitForSlurmAvailable(*slurm.Timeouts.NORMAL),
        ndd.RunNDDViaSlurm(),
        config.CustomizeWindowsSetup(),
        boot.SetBootIntoLocalWin7(),
        ssh.RebootLinux(*ssh.Timeouts.TINY),
        ssh.WaitUntilBootedIntoWindows(*ssh.Timeouts.BIG),
        boot.ResetBoot(),
        ssh.RebootWindows(*ssh.Timeouts.TINY),
        ssh.WaitForSSHAvailable(*ssh.Timeouts.NORMAL),
    ]
