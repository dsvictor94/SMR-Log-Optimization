package ch.usi.da.smr.recovery;
/* 
 * Copyright (c) 2014 Università della Svizzera italiana (USI)
 * 
 * This file is part of URingPaxos.
 *
 * URingPaxos is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * URingPaxos is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with URingPaxos.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import ch.usi.da.smr.PartitionManager;
import ch.usi.da.smr.Replica;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Name: HttpRecovery<br>
 * Description: <br>
 * 
 * Creation date: Nov 6, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class FTPRecovery implements RecoveryInterface {

	private final static Logger logger = Logger.getLogger(FTPRecovery.class);

	public final int nodeID;
	public final String token;
	private final PartitionManager partitions;
	
	private final int max_ring = 20;

	//private HttpServer httpd;

	private final String path;
	private final String prefix;
	
	public static final String snapshot_file = "snapshot";
	public static final String snapshot_file_ext = ".ser";	

	public static final String state_file = "snapshot"; // only used to quickly decide if remote snapshot is newer
	public static final String state_file_ext = ".state"; // only used to quickly decide if remote snapshot is newer
	
	private int current_checkpoint_file = 0;	// alternates between 0 and 1 upon a checkpoint finishes.
	private String endCPMark = "_ECP_\n";
	
	private FTPClient ftpClient;

	private boolean download_active = false;

	private final String server;
	private final int port;
	
    private String user = "admin";
    private String pass = "admin";
	
    private int chunkSize = 4096;
	
	public FTPRecovery(int nodeID, String token, String prefix,PartitionManager partitions, String server,int port){
	    this.server = server;
	    this.port = port;
		this.nodeID = nodeID;
		this.token = token;
		this.prefix = prefix;
		this.partitions = partitions;
		this.path = prefix + "/" + token + "/" + nodeID;
		File dir = new File(path);
		dir.mkdirs();	    
	    connect();
	}
	
    public void connect(){
    	try{
	    	ftpClient = new FTPClient();
	        ftpClient.connect(server, port);
	        ftpClient.login(user, pass);
	        ftpClient.enterLocalPassiveMode();
		    ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
    	} catch (IOException ex) {
	        System.out.println("Error: " + ex.getMessage());
	        ex.printStackTrace();
	    } 	    
    }

	/**
	 * This method saves checkpoint files locally and transfer them to a FTP server.	 
	 */
	public boolean storeState(Map<Integer,Long> instances, Map<String,byte[]> db){
		int read = 0;
   		byte[] bytesIn = new byte[chunkSize];
   		InputStream inputStream;
   		OutputStream outputStream;
   		boolean completed;
   		
		try {
			synchronized(db){
				logger.debug("Replica start storing state ... ");

				for(Entry<Integer,Long> e : instances.entrySet()){
					db.put("r:" + e.getKey(),e.getValue().toString().getBytes());
				}
				
				FileOutputStream fs = new FileOutputStream(path + "/" + snapshot_file + String.valueOf(current_checkpoint_file) + snapshot_file_ext);
		        ObjectOutputStream os = new ObjectOutputStream(fs);
		        os.writeObject(db);
		        os.flush();
		        fs.write(endCPMark.getBytes());
		        fs.getChannel().force(false); // fsync  // false forces written of data content only - metadata information is not updated
		        os.close();
				
	    		inputStream = new FileInputStream(path + "/" + snapshot_file + String.valueOf(current_checkpoint_file) + snapshot_file_ext);
		        outputStream = ftpClient.storeFileStream(snapshot_file + String.valueOf(current_checkpoint_file) + snapshot_file_ext);

		        //System.out.println("Opened the file: "+path + "/" + state_file + String.valueOf(current_checkpoint_file) + state_file_ext);
		        //System.out.println("Sending through FTP: buffer size "+chunkSize);
		        
		        while ((read = inputStream.read(bytesIn)) != -1) {
		            outputStream.write(bytesIn, 0, read);
		        }
		        inputStream.close();
		        outputStream.close();
		        
		        completed = ftpClient.completePendingCommand();
		        if (!completed){
		        	//System.out.println("Sending of checkpoint file was not completed");
		        	return false;
		        }
				fs = new FileOutputStream(path + "/" + state_file + String.valueOf(current_checkpoint_file) + state_file_ext);
				for(Entry<Integer, Long> e : instances.entrySet()){
					fs.write((e.getKey() + "=" + e.getValue() + "\n").getBytes());
				}
				fs.write(endCPMark.getBytes());
				fs.getChannel().force(false);
				fs.close();

	    		inputStream = new FileInputStream(path + "/" + state_file + String.valueOf(current_checkpoint_file) + state_file_ext);
		        outputStream = ftpClient.storeFileStream(state_file + String.valueOf(current_checkpoint_file) + state_file_ext);

		        while ((read = inputStream.read(bytesIn)) != -1) {
		            outputStream.write(bytesIn, 0, read);
		        }
		        inputStream.close();
		        outputStream.close();
		        
		        completed = ftpClient.completePendingCommand();
		        if (!completed)
		        	return false;
				
		     	current_checkpoint_file = 1 - current_checkpoint_file; // flips between 0 and 1
		        logger.debug("... state stored up to instance: " + instances);
			}
	        return true;
		} catch (IOException e) {
			logger.error(e);
		}
		return false;
	}
	
	// TODO: right now it is implemented a installState from a local file system!!
	public Map<Integer,Long> installState(String token,Map<String,byte[]> db) throws Exception {
		
		Map<Integer,Long> instances = new HashMap<Integer,Long>();
		String node = null;
		InputStreamReader isr;
		String line;
		Map<Integer, Long> state = new HashMap<Integer, Long>();	
		// local
		try{
			isr = new InputStreamReader(new FileInputStream(path + "/" + state_file + String.valueOf(current_checkpoint_file) + state_file_ext));
			BufferedReader bin = new BufferedReader(isr);	
			while ((line = bin.readLine()) != null){
				String[] s = line.split("=");
				state.put(Integer.parseInt(s[0]),Long.parseLong(s[1]));
			}
			logger.debug("Replica found local snapshot: " + state);
			//logger.info("Replica found local snapshot: " + state);
			bin.close();
		}catch (FileNotFoundException e){
			logger.debug("No local snapshot present.");
			//logger.info("No local snapshot present.");
		}
		
		/*
		
		// remote TODO: must ask min. GSQ replicas
		for(String n : partitions.getReplicaIDs(token)){
			try{
				isr = new InputStreamReader(new FileInputStream(prefix + "/" + token + "/" + n + "/" + state_file));
				BufferedReader in = new BufferedReader(isr);
				Map<Integer, Long> nstate = new HashMap<Integer, Long>();
				while ((line = in.readLine()) != null){
					String[] s = line.split("=");
					nstate.put(Integer.parseInt(s[0]),Long.parseLong(s[1]));
				}
				if(state.isEmpty()){
					state = nstate;
					node = n;
				}else if(Replica.newerState(nstate,state)){
					state = nstate;
					node = n;
				}
				logger.info("Replica found remote snapshot: " + nstate + " (" + n + ")");
				in.close();
			}catch(Exception e){
				logger.error("Error getting state from " + n,e);
			}
		}
		
		*/
		
		synchronized(db){
			InputStream in;
			/*
			if(node != null){
				logger.info("Use remote snapshot from host " + node);
				in = new FileInputStream(new File(prefix + "/" + token + "/" + node + "/" + snapshot_file));
			}else{
			*/
				in = new FileInputStream(new File(path + "/" + snapshot_file + String.valueOf(current_checkpoint_file) + snapshot_file_ext));
			//}
			ObjectInputStream ois = new ObjectInputStream(in);
			@SuppressWarnings("unchecked")
			Map<String,byte[]> m = (Map<String,byte[]>) ois.readObject();
			ois.close();
			in.close();
			db.clear();
			db.putAll(m);
			byte[] b = null;
			for(int i = 1;i<max_ring+1;i++){
				if((b = db.get("r:" + i)) != null){
					instances.put(i,Long.valueOf(new String(b)));
				}
			}
		}
		logger.debug("Replica installed snapshot instance: " + instances);
		//logger.info("Replica installed snapshot instance: " + instances);
		return instances;	}

	public void close(){	    
    	try {
            if (ftpClient.isConnected()) {
                ftpClient.logout();
                ftpClient.disconnect();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }    	
	}
	
	public boolean checkFinalMark(String fileName){
		FileInputStream fis;
		byte[] buffer = new byte[endCPMark.length()];
		try {
			fis = new FileInputStream(fileName);
		    fis.getChannel().position(fis.getChannel().size() - endCPMark.length());
		    fis.read(buffer);
		    fis.close();
		    if(Arrays.equals(buffer, endCPMark.getBytes())){
		    	//System.out.println("Found a final mark");
		    	return true;	
		    }		    
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e);
		}		
		return false;
	}

}
