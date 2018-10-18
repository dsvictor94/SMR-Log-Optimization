cd ~/SMR-Log-Optimization/experiments/2018-10-04/scripts/;
export IFACE="$(/sbin/ip route get 10.1.1.0 | awk '{print $4;exit}')";
echo IFACE=$IFACE;
opt_folder='../opt/'
zoo_folder="${opt_folder}/zookeeper-3.4.12/bin"
paxos_folder="${opt_folder}/Paxos-trunk"
smr_folder="${opt_folder}/SMR-trunk"
export STATE_TRANSFER=ch.usi.da.smr.statetransfer.TCPStateTransfer;
export APPLICATION_LOGGER=ch.usi.da.smr.log.InMemory;
export TCP_STATE_TRANSFER_PORT=5555

$zoo_folder/zkServer.sh start ../config/zookeper.cfg

$paxos_folder/thriftnode.sh 1,1:PA zoo-1

$paxos_folder/thriftnode.sh 1,2:A zoo-1

$paxos_folder/thriftnode.sh 1,3:A zoo-1

$smr_folder/replica.sh 1,4,0 0 zoo-1

$smr_folder/client.sh 1,1 zoo-1

$smr_folder/replica.sh 1,5,0 0 zoo-1


start 1 10000 200 1

1538857985/322587054 = 4.7703649787508215
20960803182/322587054 = 64.97719893619785

