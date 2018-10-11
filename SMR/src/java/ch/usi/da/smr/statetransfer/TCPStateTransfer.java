package ch.usi.da.smr.statetransfer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.xml.crypto.Data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import ch.usi.da.smr.log.LoggerInterface;
import ch.usi.da.smr.message.Message;

public class TCPStateTransfer extends Thread implements StateTransferInterface {

    private LoggerInterface logger;
    private ServerSocket serverSocket;

    @Override
    public void init(LoggerInterface logger) {
        this.logger = logger;
        int port = Integer.parseInt(System.getenv("TCP_STATE_TRANSFER_PORT"));
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.start();
    }

    @Override
    public Iterable<Message> restore(int ring, long from, long to) {
        for(String host_port : System.getenv("TCP_STATE_TRANSFER_HOSTS").split("\\s+")) {
            String[] host_port_tuple = host_port.split(":", 1);
            Socket socket = null;
            try {
                socket = new Socket(host_port_tuple[0], Integer.parseInt(host_port_tuple[1]));
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeInt(ring);
                out.writeLong(from);
                out.writeLong(to);
                out.close();

                DataInputStream in = new DataInputStream(socket.getInputStream());
                int lenght = in.readInt();
                byte[] messageBytes = new byte[lenght];
                int read = in.read(messageBytes);
                if (read != lenght) continue;
                Message m = Message.fromByteArray(messageBytes);

                return Arrays.asList(m);
            } catch (Exception e) {
                e.printStackTrace();
			} finally {
                if(socket != null)
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            }
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
        try {
            while(!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                DataInputStream in = new DataInputStream(socket.getInputStream());
                int ring = in.readInt();
                long from = in.readLong();
                long to = in.readLong();
                in.close();

                Message log = null;
                synchronized (this.logger) {
                    log = this.logger.retrive(ring, from, to).iterator().next();
                }

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                byte[] messageBytes = Message.toByteArray(log);
                out.writeInt(messageBytes.length);
                out.write(messageBytes);
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}