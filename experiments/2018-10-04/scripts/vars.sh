#!/bin/bash

user=dsvictor
ethernet="em4"

zoo_nodes=( zoo-1 )
proposer_nodes=( ring-1 )
acceptor_nodes=( ring-2 ring-3 )
replica_nodes=( ring-4 )
client_nodes=( node-1 ) # node-2 node-3 node-4 node-5 node-6 node-7 node-8 node-9 node-10 node-11 node-12 node-13 node-14 node-15 node-16 node-17 node-18 node-19 node-20 )

zoo=`join_by , ${zoo_nodes[@]}`

pattern=(1) # 2 4 8 12 16 24 32 40 48 56 64 72 80)
#pattern=(8 16)

pause=40
# DEBUG=TRUE

duration=120
max_threads_per_node=4
requests_p_thread=100000
cmd_size=200
cmds_per_batch=1
