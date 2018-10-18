package ch.usi.da.smr.log;

import java.util.ArrayList;
import java.util.List;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.transport.ABListener;

public class Dummy implements LoggerInterface {

    @Override
    public void initialize(ABListener ab) {}

    @Override
	public void store(Message m) {}

    @Override
	public void commit(int ring) {}

	@Override
	public Message retrive(int ring, long from) {
		return null;
	}

	@Override
	public void truncate(int ring, long instance) {}
    
    @Override
    public Integer size() {
        return 0;
    }

}
