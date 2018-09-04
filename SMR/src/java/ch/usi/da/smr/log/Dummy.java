package ch.usi.da.smr.log;

import java.util.ArrayList;

import ch.usi.da.smr.message.Message;

public class Dummy implements LoggerInterface {

    private final Iterable<Message> emptyIterator = new ArrayList<>();

    @Override
    public long getLastCommitedInstance(int ring) {
        return 0;
    }

    @Override
	public void store(Message m) {
    }

    @Override
	public void commit(int ring) {
    }

	@Override
	public Iterable<Message> retrive(int ring, long from, long to) {
		return emptyIterator;
	}

	@Override
	public void truncate(int ring, long instance) {
    }
    
    @Override
    public Integer count() {
        return 0;
    }

}
