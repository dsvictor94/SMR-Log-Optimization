package ch.usi.da.smr.log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import ch.usi.da.smr.message.Command;

public class Log {
    private ArrayList<Command> data;
    private ArrayList<Long> index;
    private long indexOffset;
    private long dataOffset;

    public Log() {
        this.dataOffset = 0;
        this.data = new ArrayList<Command>();

        this.indexOffset = 0;
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
        int toIndex = (int) (index.get((int) (end - indexOffset)) - dataOffset);

        return data.subList(fromIndex, toIndex);
    }

    public void truncate(Long at) {
        this.index.subList(0, (int) (at - indexOffset)).clear();
        this.indexOffset = at;

        long i = this.index.get((int) (at - indexOffset));
        this.data.subList(0, (int) (i - dataOffset)).clear();
        this.dataOffset = i;
    }

    public void add(long at, Collection<Command> commands) {
        long cmdIndex = data.size() + dataOffset;
        this.index.add((int) (at - indexOffset), cmdIndex);
        this.data.addAll(commands);
    }

    public int size() {
        return this.data.size();
    }

    
}