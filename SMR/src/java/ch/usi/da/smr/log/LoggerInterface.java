package ch.usi.da.smr.log;

import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.transport.ABListener;

public interface LoggerInterface {

	public void initialize(ABListener ab);

	public void store(Message m);

	public void commit(int ring);

	public Iterable<Message> retrive(int ring, long from, long to);

	public void truncate(int ring, long instance);

	public Integer size();
}
