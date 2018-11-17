package ch.usi.da.smr.log;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.transport.ABListener;

public interface LoggerInterface {

	public void initialize(ABListener ab);

	public void store(Message m);

	public void commit(int ring);

	public Message retrive(int ring, long from);

	public void truncate(int ring, long instance);

	public void serialize(int ring, long from, OutputStream out) throws IOException;

	public void install(InputStream in) throws IOException;

	public Integer size();
}
