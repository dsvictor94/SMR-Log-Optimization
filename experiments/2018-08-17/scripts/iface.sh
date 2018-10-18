

if [ $# -ne 1 ]; then
    echo "usage: $0 TARGET_HOST"
    exit 1
fi

host_ip=$(getent ahosts "$1" | awk '{print $1; exit}')
ip route get "$host_ip" | grep -Po '(?<=(dev )).*(?= src| proto)'