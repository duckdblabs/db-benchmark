# script to format mount and copy data.

# remove a leftover instance mount
rm -rf /var/lib/mount/db-benchmark-metal

# format the mount

source path.env

mount_name=$(sudo lsblk | awk '
NR > 1 && $1 ~ /^nvme/ && $7 == "" {
    # Convert SIZE column to bytes for comparison
    size = $4;
    unit = substr(size, length(size));
    value = substr(size, 1, length(size)-1);
    if (unit == "G") { value *= 1024^3; }
    else if (unit == "T") { value *= 1024^4; }
    else if (unit == "M") { value *= 1024^2; }
    else if (unit == "K") { value *= 1024; }
    else { value *= 1; }

    # Keep track of the largest size
    if (value > max) {
        max = value;
        largest = $1;
    }
}
END { if (largest) print largest; else print "No match found"; }
')

if [ -z "${MOUNT_POINT}" ]; then
    echo "Error: Environment variable MOUNT_POINT is not set. Set it by running"
    echo "source path.env"
    exit 1
fi

sudo mkfs -t xfs /dev/$mount_name

sudo rm -rf $MOUNT_POINT
sudo mkdir $MOUNT_POINT
sudo mount /dev/$mount_name $MOUNT_POINT

# make clone of repo on mount
sudo mkdir $MOUNT_POINT/db-benchmark-metal
sudo chown -R ubuntu:ubuntu $MOUNT_POINT


git clone $(git remote get-url origin) $MOUNT_POINT/db-benchmark-metal
cd $MOUNT_POINT/db-benchmark-metal