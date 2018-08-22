#!//bin/bash

if [ $# -ne 1 ]; then
    echo "usage: $0 TAR_GZ_FILE"
    exit 1
fi

function clear () {
    declare -A types=( ['int']='[0-9]+' ['float']='[0-9]*(.[0-9]+)?' ['+float']='([0-9]*[1-9][0-9]*(.[0-9]+)?) | ([0-9]*(.[0-9]*[1-9][0-9]*))' )
    
    p=`for i in "$@"; do echo -n "(${types["$i"]})\s+"; done`
    grep -x -E "${p::-3}" -
}

function discart () {
    head -n "-$1" | tail -n "+$(($1 + 1))"
}

function average () {
    awk "{ total += \$$1 } END { printf \"%.2f\n\", total/NR }"
}

function percentile () {
    awk "{ all[NR] = \$$2 } END{ print all[int(NR*$1 - 0.5)] }"
}

WORK_DIR="$(mktemp -d -t resume-XXXXXXX)"

if [ ! -d "$WORK_DIR" ]; then
    echo "Can not reate a temporary file"
    exit 1
fi

tar -xzf "$1" --strip 2 -C "$WORK_DIR"

cd "$WORK_DIR"


printf '\n%-10s ' "#"

nodes=`ls | grep -E "(replica)|(client)" | cut -d_ -f3 | sort -V | uniq`
for node in $nodes; do
    printf '%-10s ' "$node"
done
printf '%-10s ' "replica"
printf '%-10s ' "client"

for row in `ls | cut -d_ -f2 | sort | uniq`; do
    printf '\n%-10s ' "$row"
    replica_values=()
    client_values=()
    for node in $nodes; do
        value="-"
        file=$(echo *_${row}_${node}_*.out)

        if [[ "$file" == "replica"* ]]; then
            value="$(cat "$file" | clear int +float | discart 10 | average 2)"
            replica_values+=("$value")
        fi

        if [[ "$file" == "client"* ]]; then
            value="$(cat "$file" | clear int int | discart 20 | percentile 0.90 2)"
            client_values+=("$value")
        fi
        
        printf '%-10s ' "${value:-?}"
    done

    printf '%-10s ' "$(printf '%s\n' "${replica_values[@]}" | average 1 )"
    printf '%-10s ' "$(printf '%s\n' "${client_values[@]}" | average 1 )"
done

echo ''