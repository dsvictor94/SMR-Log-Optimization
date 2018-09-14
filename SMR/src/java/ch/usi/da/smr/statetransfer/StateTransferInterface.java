package ch.usi.da.smr.statetransfer;

import ch.usi.da.smr.log.LoggerInterface;
import ch.usi.da.smr.message.Message;

public interface StateTransferInterface {

	public void init(LoggerInterface logger);

    public Iterable<Message> restore(int ring, long from, long to);
    
    public void close();
}