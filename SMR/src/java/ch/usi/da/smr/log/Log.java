package ch.usi.da.smr.log;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;

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

    public ArrayList<Command> slice(long start, long end) {
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

    public void serialize(long from, OutputStream outStream) throws IOException {
        ArrayList<Command> sliceData;
        ArrayList<Long> sliceIndex;

        synchronized(this) {
            long lastInstance = this.getLastInstance();
            if (from > lastInstance)
                throw new IOException("there is no log after "+lastInstance+": request >= "+from);
            
            long firstInstance = this.getFirstInstance();
            if (from < firstInstance)
                throw new IOException("there is no log before "+firstInstance+": request >= "+from);

            sliceIndex = new ArrayList<>(this.index.subList((int)(from - this.indexOffset), this.index.size()));
            sliceData = this.slice(from, lastInstance);
        }

        byte[] data = SerializationUtils.serialize(sliceIndex);
        sliceIndex = null;

        DataOutputStream out = new DataOutputStream(outStream);

        out.writeLong(from);
        out.writeInt(data.length);
        out.write(data);
        out.writeInt(sliceData.size());
        for(Command cmd: sliceData){
            byte[] cmdData = Command.toByteArray(cmd);
            out.writeInt(cmdData.length);
            out.write(cmdData);
        }
    }

    public void install(InputStream inStream) throws IOException {
        DataInputStream in = new DataInputStream(inStream);
        synchronized(this) {
            long from = in.readLong();
            if(from != this.getLastInstance() + 1)
                throw new IOException("The log received starts at "+from+" but the current end at " + this.getLastInstance());
            int lenght = in.readInt();
            byte[] data = new byte[lenght];
            in.readFully(data);
            int qtdy = in.readInt();
            for(int i=0; i<qtdy; i++) {
                int size = in.readInt();
                byte[] cmdData = new byte[size];
                in.readFully(cmdData);
                this.data.add(Command.fromByteArray(cmdData));
            }
            this.index.addAll((ArrayList<Long>)SerializationUtils.deserialize(data));
        }
    }

    @Override
    public String toString() {
        return "Log<"+this.indexOffset+"+"+this.index.size()+">"+this.data+":"+this.index;
    }


    public static void main(String[] args) throws IOException {
        Log log = new Log();
        log.add(1, Arrays.asList(new Command(1, CommandType.GET, "a", "b".getBytes())));
        log.add(2, Arrays.asList(new Command(1, CommandType.GET, "b", "c".getBytes())));
        log.add(3, Arrays.asList(new Command(1, CommandType.GET, "c", "d".getBytes())));

        System.out.println(log);

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        log.serialize(3, outStream);

        InputStream inStream = new ByteArrayInputStream(outStream.toByteArray());

        Log log2 = new Log();
        log2.add(1, Arrays.asList(new Command(1, CommandType.GET, "a", "b".getBytes())));
        log2.add(2, Arrays.asList(new Command(1, CommandType.GET, "b", "c".getBytes())));

        log2.truncate(2l);

        System.out.println(log2);

        
        log2.install(inStream);

        System.out.println(log2);
    }
    
}