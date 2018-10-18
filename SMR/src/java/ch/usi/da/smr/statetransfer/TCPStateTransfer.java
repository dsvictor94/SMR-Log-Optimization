package ch.usi.da.smr.statetransfer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import ch.usi.da.smr.PartitionManager;
import ch.usi.da.smr.log.LoggerInterface;
import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.Message;

public class TCPStateTransfer extends Thread implements StateTransferInterface {

    private final static Logger debug = Logger.getLogger(TCPStateTransfer.class);

    private LoggerInterface logger;
    private PartitionManager partitions;
    private String token;
    private ServerSocket serverSocket;

    private boolean restoring = false;

    @Override
    public void init(LoggerInterface logger, PartitionManager partitions, String token) {
        this.logger = logger;
        this.partitions = partitions;
        this.token = token;

        int port = Integer.parseInt(System.getenv("TCP_STATE_TRANSFER_PORT"));
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.start();
    }

    @Override
    public Message restore(int ring, long from) {
        restoring = true;
        try {
            int port = Integer.parseInt(System.getenv("TCP_STATE_TRANSFER_PORT"));
            for(String host : partitions.getReplicas(token)) {
                Socket socket = null;
                try {
                    socket = new Socket(host, port);
                    debug.warn("Requesting log for "+host+" of ring "+ring+" from "+from);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeInt(ring);
                    out.writeLong(from);

                    debug.warn("Waiting of "+host+" of ring "+ring+" from "+from);
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    int size = in.readInt();
                    if(size < 1) {
                        debug.info("Host "+host+" refused to send log of ring "+ring+" from "+from);
                    } else {
                        long to = in.readLong();
                        ArrayList<Command> cmds = new ArrayList<>();
                        byte[] cmdBytes;
                        for(int i=0; i<size; i++){
                            int lenght = in.readInt();
                            cmdBytes = new byte[lenght];
                            int read = 0;
                            while((read += in.read(cmdBytes, read, lenght-read)) != lenght);

                            if(read != lenght) {
                                debug.warn("Inconsistent command read ("+read+") != lenght ("+lenght+")");
                            }
                            cmds.add(Command.fromByteArray(cmdBytes));
                        }

                        cmdBytes = null;
                        
                        Message m = new Message(0, "", "", cmds);
                        m.setRing(ring);
                        m.setInstance(to);
                        return m;
                    }
                } catch (Exception e) {
                    debug.info("Could not recovery from "+host, e);
                } finally {
                    if(socket != null && !socket.isClosed())
                        try {
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                }
            }
        } finally {
            restoring = false;
        }

        return null;
    }

    @Override
	public void close() {
		try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void run() {
        while(!serverSocket.isClosed()) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                
                DataInputStream in = new DataInputStream(socket.getInputStream());
                int ring = in.readInt();
                long from = in.readLong();

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                debug.info("Serving a recovery request from ring "+ring+" from "+from);

                if(restoring) { // avoid restore from itself or from a unstable replica
                    debug.info("Not a stable replica. Skip");
                    out.writeInt(0);
                } else {
                    Message log = this.logger.retrive(ring, from);

                    if(log == null) {
                        debug.info("Do not have message from ring "+ring+" from "+from+". Skip");
                        out.writeInt(0);
                    } else {
                        List<Command> cmds = log.getCommands();
                        out.writeInt(cmds.size());
                        out.writeLong(log.getInstnce());

                        for(Command cmd: cmds) {
                            byte[] cmdBytes = Command.toByteArray(cmd);
                            out.writeInt(cmdBytes.length);
                            out.write(cmdBytes);
                        }

                        debug.info("Response with mensage of ring "+ring+" from "+from+" to "+log.getInstnce()+"with "+cmds.size()+"commands");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if(socket != null && !socket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}