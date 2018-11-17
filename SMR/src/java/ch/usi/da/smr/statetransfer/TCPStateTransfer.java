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
                    
                    logger.install(in);
                    return logger.retrive(ring, from);
                } catch (IOException e) {
                    debug.info("Could not recovery from "+host);
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
                    out.writeInt(-1);
                } else {
                    try {
                        logger.serialize(ring, from, out);
                        debug.info("Response with log");
                    } catch(IOException ex) {
                        debug.info("Do not have message from ring "+ring+" from "+from+". Skip");
                        out.writeInt(-1);
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