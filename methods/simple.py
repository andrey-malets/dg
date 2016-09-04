from common import method
from stages import basic, boot, config, ndd, network, ssh


class SimpleMethod(method.Method):
    'method for deploying pre-configured machines'
    name = 'simple'

    stages = [
        basic.InitHosts(),
        basic.ExcludeBannedHosts(),
        ssh.CheckIsAccessible(*ssh.Timeouts.TINY),
        boot.SetBootIntoCOWMemory(),
        ssh.RebootHost(*ssh.Timeouts.TINY),
        ssh.WaitUntilBootedIntoCOWMemory(*ssh.Timeouts.NORMAL),
        boot.ResetBoot(),
        config.StoreCOWConfig(),
        network.EnsureNetworkSpeed(),
        ndd.RunNDD(),
        config.CustomizeWindowsSetup(),
        boot.SetBootIntoLocalWindows(),
        ssh.RebootHost(*ssh.Timeouts.TINY),
        ssh.WaitUntilBootedIntoLocalWindows(*ssh.Timeouts.BIG),
        boot.SetBootIntoLocalLinux(),
        ssh.RebootHost(*ssh.Timeouts.TINY),
        ssh.WaitUntilBootedIntoLocalLinux(*ssh.Timeouts.NORMAL),
        boot.ResetBoot(),
        ssh.MaybeRebootLocalLinux(*ssh.Timeouts.TINY),
        ssh.CheckIsAccessible(*ssh.Timeouts.NORMAL),
    ]
