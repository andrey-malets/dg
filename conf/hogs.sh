GROUP=hogs
METHOD=amt
LOCAL_ADDRESS=172.16.12.101

part() {
    echo "/dev/disk/by-partlabel/$1"
}
LOCK=("/root/cow/conf/host/hamming.urgu.org.sh" "/root/xen/windows7.cfg")
NDD=("/var/lib/cow/image64.urgu.org/cow-image64-local:$(part cow-image64-local)"
     "/tmp/windows7:$(part windows7)")

BAN=()
