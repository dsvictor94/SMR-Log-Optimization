package ch.usi.da.smr.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import ch.usi.da.smr.message.Message;

public class InMemory implements LoggerInterface {

    private Map<Integer, Long> lastInstance;
    private Map<Integer, Long> firstInstance;

    private Map<Integer, ArrayList<Message>> log;

    public InMemory() {
        this.lastInstance = new HashMap<>();
        this.firstInstance = new HashMap<>();
        this.log = new HashMap<>();
    }


    private ArrayList<Message> getRingLog(int ring) {
        if(!this.log.containsKey(ring)){
            this.log.put(ring, new ArrayList<Message>());
        }
        return this.log.get(ring);
    }


    private int toIndex(int ring, long instance) {
        return (int) (this.firstInstance.getOrDefault(ring, 0L) - instance);
    }

    @Override
    public long getLastCommitedInstance(int ring) {
        return this.lastInstance.getOrDefault(ring, 0L);
    }

    @Override
	public void store(Message m) {
        if (m.getInstnce()-1 == this.lastInstance.getOrDefault(m.getRing(), 0L)) {
            this.getRingLog(m.getRing()).add(m);
            this.lastInstance.put(m.getRing(), m.getInstnce());
        }
    }

    @Override
	public void commit(int ring) {
        // it is a sync log
    }

	@Override
	public Iterable<Message> retrive(int ring, long from, long to) {
		return this.getRingLog(ring).subList(toIndex(ring, from), toIndex(ring, from));
	}

	@Override
	public void truncate(int ring, long instance) {
        if (instance >= this.firstInstance.getOrDefault(ring, 0L) && instance <= this.lastInstance.getOrDefault(ring, 0L)) {
            this.log.put(ring, new ArrayList<Message>(this.getRingLog(ring).subList(toIndex(ring, instance), log.size()-1)));
            this.firstInstance.put(ring, instance);
        }
    }
    
    @Override
    public Integer count() {
        Integer c = 0;
        for (ArrayList<Message> l: this.log.values()) {
            c += l.size();
        }

        return c;
    }

}
