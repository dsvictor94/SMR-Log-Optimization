package ch.usi.da.smr.statetransfer;

import ch.usi.da.smr.PartitionManager;
import ch.usi.da.smr.log.LoggerInterface;
import ch.usi.da.smr.message.Message;

public interface StateTransferInterface {

	public void init(LoggerInterface logger, PartitionManager partitions, String token);

    public Message restore(int ring, long from);
    
    public void close();
}