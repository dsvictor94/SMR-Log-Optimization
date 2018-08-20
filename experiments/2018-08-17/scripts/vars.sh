#!/bin/bash

user=victor
ethernet="wlp2s0"

zoo_nodes=( 192.168.0.105 )
proposer_nodes=( localhost )
acceptor_nodes=( localhost )
replica_nodes=( localhost )
client_nodes=( localhost localhost )

zoo=`join_by , ${zoo_nodes[@]}`

#pattern=(1 2 4 8 16 32 64 128 256 512)
pattern=(2 4)

pause=5

duration=20
max_threads_per_node=2
requests_p_thread=100000
cmd_size=200
cmds_per_batch=1