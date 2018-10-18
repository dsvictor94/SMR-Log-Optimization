package ch.usi.da.smr.log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.transport.ABListener;
import ch.usi.da.smr.transport.Receiver;


public class InMemory implements LoggerInterface {

    protected Map<Integer, Long> lastInstance;
    protected Map<Integer, Long> firstInstance;

    protected Map<Integer, Log> log;
    private ABListener ab;

    private final static Logger logger = Logger.getLogger(Log.class);

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
            // logger.info("Commit ring "+ring+" to "+this.getRingLog(ring).getLastInstance());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Override
	public Message retrive(int ring, long from) {
        Log ringLog = this.getRingLog(ring);
        synchronized (ringLog) {
            long lastInstance = ringLog.getLastInstance();
            if (from > lastInstance)
                return null;

            if (from < ringLog.getFirstInstance()) 
                from = ringLog.getFirstInstance();

            List<Command> cmds = ringLog.slice(from, lastInstance);
            Message msg = new Message(0, "", "", cmds);
            msg.setInstance(lastInstance);
            msg.setRing(ring);

            return msg;
        }
	}

	@Override
	public void truncate(int ring, long instance) {
        Log ringLog = this.getRingLog(ring);
        synchronized (ringLog) {
            ringLog.truncate(instance);
        }
    }
    
    @Override
    public Integer size() {
        Integer c = 0;
        for (Log l: this.log.values()) {
            c += l.size();
        }

        return c;
    }

    public static void main (String[] args) {
        Message GET_X = new Message(1, "", "", Arrays.asList(new Command(1, CommandType.GET, "x", new byte[0])));
        GET_X.setInstance(1);
        GET_X.setRing(1);
        Message PUT_X = new Message(2, "", "", Arrays.asList(new Command(1, CommandType.PUT, "x", new byte[]{52, 53})));
        PUT_X.setInstance(2);
        PUT_X.setRing(1);
        Message GET_Y = new Message(3, "", "", Arrays.asList(new Command(1, CommandType.GET, "y", new byte[0])));
        GET_Y.setInstance(3);
        GET_Y.setRing(1);

        InMemory logger = new InMemory();
        logger.initialize(new ABListener() {

            @Override
            public void registerReceiver(Receiver receiver) {

            }

            @Override
            public void safe(int ring, long instance) throws Exception {
                System.out.println("SAFE for "+ring+" - "+instance);
            }

            @Override
            public void close() {
                System.out.println("CLOSE");
			}
            
        });

        logger.store(GET_X);
        System.out.println("Size: " + logger.size());
        System.out.println("Message: " + logger.retrive(1, 0));

        logger.commit(1);

        logger.store(PUT_X);
        System.out.println("Size: " + logger.size());
        System.out.println("Message: " + logger.retrive(1, 1));
    }
}
