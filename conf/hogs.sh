METHOD=stdm
DG_HOSTS=(uzer-pc)
DG_GROUPS=(hogs)
LOCAL_ADDRESS=172.16.12.101
AMTPASSWD=/root/dg/amtpasswd

part() {
    echo "/dev/disk/by-partlabel/$1"
}
LOCK=("/root/cow/conf/host/hamming.urgu.org.sh" "/root/xen/windows7.cfg")
NDD=("/var/lib/cow/image64.urgu.org/cow-image64-local:$(part cow-image64-local)"
     "/tmp/windows7:$(part windows7)"
     "uzer-pc:$(part EFI):$(part EFI)+z"
     "uzer-pc:$(part macos):$(part macos)+z")
NDD_PORT=3634

ARGS=(-wd windows-data:W)
