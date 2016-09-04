from common import method
from stages import (
    amt, amtredird, basic, boot, config, disk, ndd, network, ssh, stdm
)


class StdMMethod(method.Method):
    'method for deploying hosts with Standard Manageability'
    name = 'stdm'

    stages = [
        basic.InitHosts(),
        basic.ExcludeBannedHosts(),
        amt.DetermineAMTHosts(),
        amtredird.EnsureRedirectionPossible(),
        stdm.WakeupStdMHosts(),
        amtredird.EnableRedirection(),
        boot.SetBootIntoCOWMemory(),
        stdm.ResetStdMHosts(),
        ssh.WaitUntilBootedIntoCOWMemory(*ssh.Timeouts.NORMAL),
        amtredird.DisableRedirection(),
        boot.ResetBoot(),
        disk.FreeDisk(),
        disk.ConfigureDisk(),
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
