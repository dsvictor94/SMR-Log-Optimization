#!/bin/bash

source "$(dirname $0)/common.sh"

if [ $# -gt 1 ] || ( [ $# -eq 1 ] && [ "$1" != '--skip-install' ] ); then
    echo "usage: $0 [--skip-install]"
    exit 1
fi

if [ "$1" != '--skip-install' ]; then
    if [ -d "$opt_folder" ]; then
        >&2 echo "ERROR:" "The experimentis already instaled. Delete $opt_folder to uninstall or skip instalation with '$0 --skip-install'"
        exit 1
    fi

    mkdir "$opt_folder"

    for file in `find "$resources" -type f`; do
        echo "Instaling $file in $opt_folder"
        tar -xzf "$file" -C "$opt_folder"
    done
fi

if [ `echo "$(max "${pattern[@]}") / ${max_threads_per_node} > ${#client_nodes[@]}" | bc -l` == 1 ]; then
    echo "Not enouth clients nodes. Exiting."
    exit 1
fi

echo "Starting"

zoo_folder="${opt_folder}/zookeeper-3.4.12/bin"
paxos_folder="${opt_folder}/Paxos-trunk"
smr_folder="${opt_folder}/SMR-trunk"

mkdir -p "$output_folder"

for i in ${!pattern[@]}; do

    echo "Execution ($i) - ${pattern[$i]} thread"

    echo "Starting Zookeepers"
    for node in "${zoo_nodes[@]}"; do
        exec_at --output "$(output zookeeper "$i" "$node")" "$node" sh -c "export IFACE=${ethernet}; $zoo_folder/zkServer.sh start $config_folder/zookeper.cfg"
    done
    pause

    nodeid=1

    echo "Starting Proposers"
    for node in "${proposer_nodes[@]}"; do
        exec_at --output "$(output proposer "$i" "$node")" "$node" sh -c "export IFACE=${ethernet}; ${paxos_folder}/thriftnode.sh 1,$((nodeid++)):PA ${zoo}"
    done
    pause

    echo "Starting Acceptors"
    for node in "${acceptor_nodes[@]}"; do
        exec_at --output "$(output acceptor "$i" "$node")" "$node" sh -c "export IFACE=${ethernet}; ${paxos_folder}/thriftnode.sh 1,$((nodeid++)):A ${zoo}"
    done
    pause

    echo "Starting Replicas"
    for node in "${replica_nodes[@]}"; do
        # need the sleep becouse re replica stops if stdin close
        exec_at --output "$(output replica "$i" "$node")" "$node" sh -c "export IFACE=${ethernet}; sleep 100000000 | ${smr_folder}/replica.sh 1,$((nodeid++)),0 0 ${zoo}"
    done
    pause

    echo "Starting Clients"
    remaning_threads=${pattern[$i]}
    for node in "${client_nodes[@]}"; do
        if [ $remaning_threads -le 0 ]; then
            break
        fi

        num_threads=$(( $remaning_threads > $max_threads_per_node ? $max_threads_per_node : $remaning_threads ))
        exec_at --output "$(output client $i "$node")" "$node" sh -c "export IFACE=${ethernet}; printf 'start ${num_threads} ${requests_p_thread} ${cmd_size} ${cmds_per_batch}\n' | ${smr_folder}/client.sh 1,1 ${zoo}"

        remaning_threads=$(( $remaning_threads - $num_threads ))
    done
    pause

    echo "Pausing ${duration}s to stop test"
    sleep $duration

    echo "Killing Clients"
    for node in "${client_nodes[@]}"; do
        # TODO avoid kill non running nodes
        exec_at "$node" killall -9 java
    done
    pause

    echo "Killing Replicas"
    for node in "${replica_nodes[@]}"; do
        exec_at "$node" killall -9 java
    done
    pause

    echo "Killing Acceptors"
    for node in "${acceptor_nodes[@]}"; do
        exec_at "$node" killall -9 java
    done
    pause

    echo "Killing Proposers"
    for node in "${proposer_nodes[@]}"; do
        exec_at "$node" killall -9 java
    done
    pause

    echo "Stopping Zookeeper"
    for node in "${zoo_nodes[@]}"; do
        exec_at "$node" "$zoo_folder/zkServer.sh" stop "$config_folder/zookeper.cfg"
    done
    pause

    echo "End execution ($i)"
done

timestamp=`date +%s`
dest="output_${timestamp}.tar.gz"
echo "Packaging output to $dest"

cd "${output_folder}"
mkdir "./output_${timestamp}"
mv *.out *.txt "./output_${timestamp}"
tar zvcf "output_${timestamp}.tar.gz" "./output_${timestamp}"
rm -r "./output_${timestamp}"

echo "Done"