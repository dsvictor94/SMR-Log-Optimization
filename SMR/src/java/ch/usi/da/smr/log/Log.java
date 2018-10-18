package ch.usi.da.smr.log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import ch.usi.da.smr.message.Command;

public class Log {
    private ArrayList<Command> data;
    private ArrayList<Long> index;
    private long indexOffset;
    private long dataOffset;

    private final static Logger logger = Logger.getLogger(Log.class);

    public Log() {
        this.dataOffset = 0;
        this.data = new ArrayList<Command>();

        this.indexOffset = 1;
        this.index = new ArrayList<Long>();
    }

    public long getFirstInstance() {
        return this.indexOffset;
    }

    public long getLastInstance() {
        return indexOffset + index.size() -1;
    }

    public List<Command> slice(long start, long end) {
        int fromIndex = (int) (index.get((int) (start - indexOffset)) - dataOffset);
        int toIndex;
        if(end < getLastInstance()) {
            toIndex = (int) (index.get((int) (end - indexOffset + 1)) - dataOffset);
        } else if(end == getLastInstance()) {
            toIndex = data.size();
        } else {
            throw new IndexOutOfBoundsException();
        }

        return new ArrayList<>(data.subList(fromIndex, toIndex));
    }

    public void truncate(Long at) {
        long i = this.index.get((int) (at - indexOffset));
        this.data.subList(0, (int) (i - dataOffset)).clear();
        this.dataOffset = i;

        this.index.subList(0, (int) (at - indexOffset)).clear();
        this.indexOffset = at;
    }

    public void add(long at, Collection<Command> commands) {
        // logger.info("Adding "+Arrays.toString(commands.toArray())+ " at "+at);
        if (at != this.index.size() + indexOffset) {
            // is not in sequence
            logger.info("Logging out of sequence. resetting log to "+at);
            this.index.clear();
            this.data.clear();
            this.indexOffset = at;
            this.dataOffset = 0;
        }
        long cmdIndex = data.size() + dataOffset;
        this.index.add((int) (at - indexOffset), cmdIndex);
        this.data.addAll(commands);
        // logger.info("Log index: "+indexOffset+" "+Arrays.toString(index.toArray()));
        // logger.info("Log data: "+dataOffset+" "+Arrays.toString(data.toArray()));
    }

    public int size() {
        return this.data.size();
    }

    
}