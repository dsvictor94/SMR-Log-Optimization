package ch.usi.da.smr.log;

import ch.usi.da.smr.message.Message;

public interface LoggerInterface {

	public long getLastCommitedInstance(int ring);

	public void store(Message m);

	public Iterable<Message> retrive(int ring, long from, long to);

	public void commit(int ring);

	public void truncate(int ring, long instance);

	public Integer count();
}
