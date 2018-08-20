function join_by { local IFS="$1"; shift; echo "$*"; }

source "$(dirname $0)/vars.sh"

function exec_at {
    if [ "$1" == '--output' ]; then
        output="$2"
        shift 2
    else
        output=/dev/null
    fi

    if [ $# -lt 2 ]; then
        echo "usage: exec_at [--output FILE] NODE COMMAND [ARG]..."
        return 1
    fi

    host="$1"
    shift

    cmd="nohup $(printf '%q ' "$@") &>$(printf '%q' "${output}") </dev/null &"
    
    if [ "$DEBUG" == "TRUE" ]; then
        echo "$cmd"
    else
        ssh -f -o LogLevel=ERROR -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no \
            "$user@$host" "$cmd"
    fi  
}

function output {
    name="$1"
    run="$2"
    node="${3:-`hostname`}"
    timestamp="${4:-`date +%s`}"
    
    echo "$output_folder/${name}_${run}_${node}_${timestamp}_$RANDOM.out"
}

function pause {
    if [ -n "$pause" ]; then
        sleep $pause
    else
        read -p "Press any key to continue... " -n1 -s
        echo ''
    fi
}

function max {
    max=${1}
    for n in "$@" ; do
        ((n > max)) && max=$n
    done
    echo $max
}

workfolder="$(cd ..; pwd)"

resources="$workfolder/resources"
opt_folder="$workfolder/opt"
output_folder="${workfolder}/output"
scripts_folder="${workfolder}/scripts"
config_folder="${workfolder}/config"