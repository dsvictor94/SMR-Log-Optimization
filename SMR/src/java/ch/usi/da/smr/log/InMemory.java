package ch.usi.da.smr.log;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.transport.ABListener;


public class InMemory implements LoggerInterface {

    protected Map<Integer, Long> lastInstance;
    protected Map<Integer, Long> firstInstance;

    protected Map<Integer, Log> log;
    private ABListener ab;

    public InMemory() {
        this.log = new HashMap<>();
    }

    @Override
    public void initialize(ABListener ab) {
        this.ab = ab;
    }

    protected Log getRingLog(int ring) {
        if(!this.log.containsKey(ring)){
            this.log.put(ring, new Log());
        }
        return this.log.get(ring);
    }

    @Override
	public void store(Message m) {
        this.getRingLog(m.getRing()).add(m.getInstnce(), m.getCommands());
    }

    @Override
	public void commit(int ring) {
        try {
            this.ab.safe(ring, this.getRingLog(ring).getLastInstance());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Override
	public Iterable<Message> retrive(int ring, long from, long to) {
        List<Command> commands = this.getRingLog(ring).slice(from, to);
        Message msg = new Message(0, "", "", commands);
        return Arrays.asList(msg);
	}

	@Override
	public void truncate(int ring, long instance) {
        this.getRingLog(ring).truncate(instance);
    }
    
    @Override
    public Integer size() {
        Integer c = 0;
        for (Log l: this.log.values()) {
            c += l.size();
        }

        return c;
    }

}
