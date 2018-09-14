package ch.usi.da.smr.log;

import java.util.ArrayList;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.Message;

public class InMemoryOnlyWrite extends InMemory {


    @Override
    protected int toIndex(int ring, long instance) {
        ArrayList<Message> ringLog = this.getRingLog(ring);
        if (ringLog.size() == 0)
            return 0;

        int start=0;
        int end=ringLog.size()-1;

        if (instance <= ringLog.get(start).getInstnce())
            return start;
        
        if (instance > ringLog.get(end).getInstnce())
            return end+1;
        
        while (start < end) {
            int m = start + (end - start) / 2;

            if (ringLog.get(m).getInstnce() == instance)
                return m;
            
            if (m == start)
                return end;
            
            if (ringLog.get(m).getInstnce() < instance)
                start = m;

            if (ringLog.get(m).getInstnce() > instance)
                end = m;
        }

        return end;
    }

    @Override
	public void store(Message m) {
        if (m.getInstnce()-1 == this.lastInstance.getOrDefault(m.getRing(), 0L)) {
            boolean isWrite = false;
            for(Command c: m.getCommands()) {
                if (c.isWrite()) {
                    isWrite = true;
                    break;
                }
            }
            if (isWrite) {
                this.getRingLog(m.getRing()).add(m);
            }
            this.lastInstance.put(m.getRing(), m.getInstnce());
        }
    }

	@Override
	public void truncate(int ring, long instance) {
        if (instance >= this.firstInstance.getOrDefault(ring, 0L) && instance <= this.lastInstance.getOrDefault(ring, 0L)) {
            this.log.put(ring, new ArrayList<Message>(this.getRingLog(ring).subList(toIndex(ring, instance), log.size()-1)));
            this.firstInstance.put(ring, instance);
        }
    }
    
}
