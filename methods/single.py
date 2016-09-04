from common import method
from stages import basic, boot, config, ndd, network, ssh


class SingleMethod(method.Method):
    'method for deploying single OS Linux machines'
    name = 'single'

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
        ssh.RebootHost(*ssh.Timeouts.TINY),
        ssh.WaitUntilBootedIntoLocalLinux(*ssh.Timeouts.BIG),
    ]
