package ch.usi.da.smr.log;

import java.util.ArrayList;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.Message;

public class InMemoryOnlyWrite extends InMemory {

    @Override
	public void store(Message m) {
        ArrayList<Command> commands = new ArrayList<>();
        for(Command c: m.getCommands()) {
            if (c.isWrite()) commands.add(c);
        }

        this.getRingLog(m.getRing()).add(m.getInstnce(), commands);
    }
    
}
