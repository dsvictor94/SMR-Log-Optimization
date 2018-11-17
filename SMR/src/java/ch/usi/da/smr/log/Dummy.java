package ch.usi.da.smr.log;

import java.io.InputStream;
import java.io.OutputStream;

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

    @Override
    public void serialize(int ring, long from, OutputStream out) {}

    @Override
    public void install(InputStream in) {}

}
