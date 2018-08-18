package ch.usi.da.smr;
/* 
 * Copyright (c) 2013 Università della Svizzera italiana (USI)
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipException;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.Util;
import ch.usi.da.smr.BatchSender;
import ch.usi.da.smr.Client;
import ch.usi.da.smr.PartitionManager;
import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.misc.ConfigurationFileManager;
import ch.usi.da.smr.misc.GzipCompress;
import ch.usi.da.smr.transport.Receiver;
import ch.usi.da.smr.transport.Response;
import ch.usi.da.smr.transport.UDPListener;

/**
 * Name: Client<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Client implements Receiver {
	static {
		// get hostname and pid for log file name
		String host = "localhost";
		try {
			Process proc = Runtime.getRuntime().exec("hostname");
			BufferedInputStream in = new BufferedInputStream(proc.getInputStream());
			byte [] b = new byte[in.available()];
			in.read(b);
			in.close();
			host = new String(b).replace("\n","");
		} catch (IOException e) {
		}
		int pid = 0;
		try {
			pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
		} catch (NumberFormatException | IOException e) {
		}
		System.setProperty("logfilename", host + "-" + pid + ".log");
	}
	
	private final static Logger logger = Logger.getLogger(Client.class);
	
	private int batch_size;
	//private final int batch_size = 20000; // 100 cmds (batch size) * 200 (cmd size)  // 0: disable	
	//// batch size 9000 contains 45 cmds with size 200

	//private  int cmds_per_batch = 100; // batch size = 100 cmds * 200 (cmd size) = 20000  // 0: disable	
	private  int cmds_per_batch; // batch size = 100 cmds * 200 (cmd size) = 20000  // 0: disable	


	private final PartitionManager partitions;
				
	private final UDPListener udp;
	
	private Map<Integer,Response> commands = new ConcurrentHashMap<Integer,Response>();

	private Map<Integer,List<Command>> responses = new ConcurrentHashMap<Integer,List<Command>>();

	private Map<Integer,List<String>> await_response = new ConcurrentHashMap<Integer,List<String>>();
	
//	private final List<Long> latency = Collections.synchronizedList(new ArrayList<Long>());
	
	private Map<Integer, BlockingQueue<Response>> send_queues = new HashMap<Integer, BlockingQueue<Response>>();
	
	// we need only one response per replica group
	Set<Integer> delivered = Collections.newSetFromMap(new LinkedHashMap<Integer, Boolean>(){
		private static final long serialVersionUID = -5674181661800265432L;
		protected boolean removeEldestEntry(Map.Entry<Integer, Boolean> eldest) {
	        return size() > 50000;
	    }
	});

	private final InetAddress ip;
	
	private final int port;
	
	private final Map<Integer,Integer> connectMap;
	
	private boolean compressedCmds = true;
	// table for keeping compressed commands in advance
	private final Map<Integer,byte[]> compressedCommands = new HashMap<Integer,byte[]>();
	private  int numDistinctCompressedCommands = 50000;
	private  int numDistinctCommands = 50000;
	private String cmdsCharactersPattern = "a a a b b c ";
	//private final String cmdsCharactersPattern = "a a a b c d e e f g h i i i j k l m n o p q r s s s s t u v x w y z _ ";	
	
	public Client(PartitionManager partitions,Map<Integer,Integer> connectMap) throws IOException {
		this.partitions = partitions;
		this.connectMap = connectMap;
		ip = Util.getHostAddress();
		port = 5000 + new Random().nextInt(15000);
		udp = new UDPListener(port);
		Thread t = new Thread(udp);
		t.setName("UDPListener");
		t.start();
	}

	public Client(PartitionManager partitions,Map<Integer,Integer> connectMap, int distinctKeys, String charactersPatterns) throws IOException {
		this.numDistinctCommands = distinctKeys;
		this.cmdsCharactersPattern = charactersPatterns;
		
		this.partitions = partitions;
		this.connectMap = connectMap;
		ip = Util.getHostAddress();
		port = 5000 + new Random().nextInt(15000);
		udp = new UDPListener(port);
		Thread t = new Thread(udp);
		t.setName("UDPListener");
		t.start();
	}	
	
	public void init() {
		udp.registerReceiver(this);		
	}

	public void readStdin() throws Exception {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	    String s;
	    try {
	    	int id = 0;
	    	Command cmd = null;
		    while((s = in.readLine()) != null && s.length() != 0){
		    	// read input
		    	String[] line = s.split("\\s+");
		    	if(s.startsWith("start")){
		    		cmd = null;
		    		final int concurrent_threads; // # of threads
		    		final int send_per_thread;
		    		final int value_size;
		    		//final int key_count = 50000; // 50k * 15k byte memory needed at replica
		    		String[] sl = s.split(" ");
		    		if(sl.length > 1){			    		
			    		concurrent_threads = Integer.parseInt(sl[1]);
			    		send_per_thread = Integer.parseInt(sl[2]);
			    		value_size = Integer.parseInt(sl[3]);
			    		cmds_per_batch = Integer.parseInt(sl[4]);			    		
		    		}else{
			    		concurrent_threads = 12;
			    		send_per_thread = 12500;
			    		value_size = 200;
			    		cmds_per_batch = 10;
		    		}
		    		final AtomicInteger send_id = new AtomicInteger(0);
		    		final CountDownLatch await = new CountDownLatch(concurrent_threads);		    		
		    		final AtomicLong stat_command = new AtomicLong();
		    		final AtomicLong stat_msg = new AtomicLong();	
		    		
		    		if(compressedCmds){
		    			setCompressedCommands(value_size,cmdsCharactersPattern);
		    			batch_size = cmds_per_batch;	// batch size will vary depending on the 
		    											// commands compressing ratio - so it is 
		    											// informed how many commands should be in a batch  
		    		}
		    		else
		    			batch_size = value_size*cmds_per_batch;
		    		
		    		logger.info("Start performance testing with " + concurrent_threads + " threads.");
		    		logger.info("(msgs_per_thread:" + send_per_thread + " cmd_size:" + value_size + " cmds_per_batch:"+ cmds_per_batch + " batch size:"+ batch_size +")");
		    		//logger.info("(values_per_thread:" + send_per_thread + " value_size:" + value_size + ")");
		    		for(int i=0;i<concurrent_threads;i++){
		    			
		    			Thread t = new Thread("Command Sender Multiple" + i){
							@Override
							public void run(){								
								
								String localhost="";
								try {
									localhost = InetAddress.getLocalHost().getHostName();
								} catch (UnknownHostException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
								
								int send_count = 0;
								int i = 0;
								Random rand = new Random();
								
								while(send_count < send_per_thread){
									int id;									
									List<Command> cmds = new ArrayList<Command>();								
									//int printStats = 0;
									
									for (i = 0; i <  cmds_per_batch; i++){
										id = send_id.incrementAndGet();										
										if(compressedCmds){											
											cmds.add(new Command(id,CommandType.PUT,localhost+"_user" + (id % numDistinctCommands), getCompressedCmd()));
											//cmds.add(new Command(id,CommandType.PUT,localhost+"_user" + (id % key_count), getCompressedCmd()));
										}
										else
											cmds.add(new Command(id,CommandType.PUT,localhost+"_user" + (id % numDistinctCommands), new byte[value_size]));
											//cmds.add(new Command(id,CommandType.PUT,localhost+"_user" + (id % key_count), new byte[value_size]));
										
											
									}
									
									List<Response> r = null;
																		
									try{
										long time = System.nanoTime();
										long time_aux; 
										int timeout=1000;
										if((r = sendBatch(cmds)) != null){
											//for (i = 0; i < cmds_per_batch; i++){
											for (i = 0; i < r.size(); i++){
												time_aux = System.nanoTime();
												r.get(i).getResponse(timeout); // wait response
												//System.out.println(r.get(i).toString());
												time_aux = System.nanoTime() - time_aux;
												timeout -= time_aux; 
												if(timeout <= 0)
													break;
											}
											float lat = System.nanoTime() - time;
										
											// this condition will hold once every (n*concurrent_threads) messages have been received
											//printStats = (printStats + 1) % (5*concurrent_threads);		// selects the next thread to process m
											//if(printStats == i){
											if(rand.nextInt(5*concurrent_threads) == 1){											
												// shows the current time and the latency												
										      	System.out.printf("%d  %.0f\n",System.nanoTime(),lat);
											}																						
											stat_command.addAndGet(cmds_per_batch);
											stat_msg.incrementAndGet();
											r.clear();
										}
									} catch (Exception e){
										logger.error("Error in send thread!",e);
									}
									send_count++;	// counting number of messages sent
									//send_count+=cmds_per_batch;	// counting number of commands sent
								}
								await.countDown();
								logger.debug("Thread terminated.");
							}
						};						
		    			
						t.start();
		    		}
		    		await.await(); // wait until finished
		    		logger.info("Performance testing generated " +stat_command.get() + " commands and "+stat_msg.get() +" messages.");
		    		
		    	}else if(line.length > 3){
		    		try{
		    			String arg2 = line[2];
		    			if(arg2.equals(".")){ arg2 = ""; } // simulate empty string
		    			cmd = new Command(id,CommandType.valueOf(line[0].toUpperCase()),line[1],arg2.getBytes(),Integer.parseInt(line[3]));
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else if(line.length > 2){
		    		try{
		    			if(compressedCmds)
		    				cmd = new Command(id,CommandType.valueOf(line[0].toUpperCase()),line[1],compressedCmd(line[2]));		    			
		    			else
		    				cmd = new Command(id,CommandType.valueOf(line[0].toUpperCase()),line[1],line[2].getBytes());
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else if(line.length > 1){
		    		try{
		    			cmd = new Command(id,CommandType.valueOf(line[0].toUpperCase()),line[1],new byte[0]);
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else{
		    		System.out.println("Add command: <PUT|GET|GETRANGE|DELETE> key <value>");
		    	}
		    	// send a command
		    	if(cmd != null){
		    		Response r = null;
		        	if((r = send(cmd)) != null){
		        		List<Command> response = r.getResponse(1000); // wait response
		        		if(!response.isEmpty()){
		        			for(Command c : response){
		    	    			if(c.getType() == CommandType.RESPONSE){		    	    				
		    	    				if(c.getValue() != null){
		    	    					GzipCompress gzip = new GzipCompress();
		    	    					String tmp;
		    	    					try{
		    	    						tmp = gzip.decompress(c.getValue());
		    	    					}
		    	    					catch(ZipException e){	// it holds when a reply is not a compressed value
		    	    						tmp = new String(c.getValue());
		    	    					}
		    	    					System.out.println("  -> " + tmp);
		    	    				}else{
		    	    					System.out.println("<no entry>");
		    	    				}
		    	    			}			    				
		        			}
		        			id++; // re-use same ID if you run into a timeout
		        		}else{
		        			System.err.println("Did not receive response from replicas: " + cmd);
		        		}
		        	}else{
		        		System.err.println("Could not send command: " + cmd);
		        	}
		    	}
		    }
		    in.close();
	    } catch(IOException e){
	    	e.printStackTrace();
	    } catch (InterruptedException e) {
		}
	    stop();
	}

	public void stop(){
		udp.close();
	}

	/**
	 * Send a command (use same ID if your Response ended in a timeout)
	 * 
	 * (the commands will be batched to larger Paxos instances)
	 * 
	 * @param cmd The command to send
	 * @return A Response object on which you can wait
	 * @throws Exception
	 */
	public Response send(Command cmd) throws Exception {
		Response r = new Response(cmd);
		commands.put(cmd.getID(),r);
		int ring = -1;
    	if(cmd.getType() == CommandType.GETRANGE){
    		ring  = partitions.getGlobalRing();
    		List<String> await = new ArrayList<String>();
    		for(Partition p : partitions.getPartitions()){
    			await.add(p.getID());
    		}
    		await_response.put(cmd.getID(),await);
    	}else{
    		ring = partitions.getRing(cmd.getKey());
			// special case for EC2 inter-region app;
			String single_part = System.getenv("PART");
			if(single_part != null){
				ring = Integer.parseInt(single_part);
			}
    	}
		if(ring < 0){ System.err.println("No partition found for key " + cmd.getKey()); return null; };
    	synchronized(send_queues){
		if(!send_queues.containsKey(ring)){
    			send_queues.put(ring,new LinkedBlockingQueue<Response>());
    			Thread t = new Thread(new BatchSender(ring,this));
    			t.setName("BatchSender-" + ring);
    			t.start();
    	}}
    	send_queues.get(ring).add(r);
    	return r;		
	}

	
	public List<Response> sendBatch(List<Command> cmds) throws Exception {
		List <Response> r = new ArrayList<Response>(); 
		int i = 0;
		int ring = -1;
		
		for (Command c: cmds){			
			r.add(new Response(c));
			commands.put(c.getID(),r.get(i));
	    	if(c.getType() == CommandType.GETRANGE){
	    		ring  = partitions.getGlobalRing();
	    		List<String> await = new ArrayList<String>();
	    		for(Partition p : partitions.getPartitions()){
	    			await.add(p.getID());
	    		}
	    		await_response.put(c.getID(),await);
	    	}else{
	    		ring = partitions.getRing(c.getKey());
				// special case for EC2 inter-region app;
				String single_part = System.getenv("PART");
				if(single_part != null){
					ring = Integer.parseInt(single_part);
				}
	    	}
	    	if(ring < 0){ System.err.println("No partition found for key " + c.getKey()); return null; };
	    	
	    	i++;
		}
    	
		synchronized(send_queues){
    		if(!send_queues.containsKey(ring)){
        			send_queues.put(ring,new LinkedBlockingQueue<Response>());        			
        			Thread t = new Thread(new BatchSender(ring,this,batch_size,compressedCmds));
        			t.setName("BatchSender-" + ring);
        			t.start();
        	}}
	    	
		// Threads' commands are added contiguously in the queue
		synchronized(send_queues.get(ring)){
			//for(i = 0; i < r.size(); i++){
			for(Response r_tmp: r){
				//logger.debug("Adding message to sender queue:"+r_tmp);
		    	send_queues.get(ring).add(r_tmp); 	
		    	i++;	    
			}
		}
    	return r;
	}	

/*	
	
	public List<Response> sendBatch(List<Command> cmds) throws Exception {
		List <Response> r = new ArrayList<Response>(); 
		int i = 0;
		int ring = -1;
		for (Command c: cmds){
			r.add(new Response(c));
			commands.put(c.getID(),r.get(i));
	    	if(c.getType() == CommandType.GETRANGE){
	    		ring  = partitions.getGlobalRing();
	    		List<String> await = new ArrayList<String>();
	    		for(Partition p : partitions.getPartitions()){
	    			await.add(p.getID());
	    		}
	    		await_response.put(c.getID(),await);
	    	}else{
	    		ring = partitions.getRing(c.getKey());
				// special case for EC2 inter-region app;
				String single_part = System.getenv("PART");
				if(single_part != null){
					ring = Integer.parseInt(single_part);
				}
	    	}
	    	if(ring < 0){ System.err.println("No partition found for key " + c.getKey()); return null; };
	    	i++;
		}
		
    	synchronized(send_queues){
		if(!send_queues.containsKey(ring)){
    			send_queues.put(ring,new LinkedBlockingQueue<Response>());
    			Thread t = new Thread(new BatchSender(ring,this,batch_size));
    			t.setName("BatchSender-" + ring);
    			t.start();
    	}}
    	
    	for(Response resp: r)
    		send_queues.get(ring).add(resp); 	

    	return r;		
	}	
	
*/	
	
	@Override
	public synchronized void receive(Message m) {
		logger.debug("Client received ring " + m.getRing() + " instnace " + m.getInstnce() + " (" + m + ")");
		
		// filter away already received replica answers
		if(delivered.contains(m.getID())){
			return;
		}else{
			delivered.add(m.getID());
		}
		
		// un-batch response (multiple responses per command_id)
		for(Command c : m.getCommands()){
			if(!responses.containsKey(c.getID())){
				List<Command> l = new ArrayList<Command>();
				responses.put(c.getID(),l);
			}
			List<Command> l = responses.get(c.getID());
			if(!c.getKey().isEmpty() && !l.contains(c)){
				l.add(c);
			}
		}
		
		// set responses to open commands
		Iterator<Entry<Integer, List<Command>>> it = responses.entrySet().iterator();
		while(it.hasNext()){
			Entry<Integer, List<Command>> e = it.next();
			if(commands.containsKey(e.getKey())){
				if(await_response.containsKey(e.getKey())){ // handle GETRANGE responses from different partitions
					await_response.get(e.getKey()).remove(m.getFrom());
					if(await_response.get(e.getKey()).isEmpty()){
						commands.get(e.getKey()).setResponse(e.getValue());
						commands.remove(e.getKey());
						await_response.remove(e.getKey());
						it.remove();
					}
				}else{
					commands.get(e.getKey()).setResponse(e.getValue());
					commands.remove(e.getKey());
					it.remove();
				}
			}
		}
	}

	public PartitionManager getPartitions() {
		return partitions;
	}

	public Map<Integer, BlockingQueue<Response>> getSendQueues() {
		return send_queues;
	}

	public InetAddress getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public Map<Integer, Integer> getConnectMap() {
		return connectMap;
	}

	public void setCompressedCommands(int stringSize,String charactersPattern){
		GzipCompress gzip = new GzipCompress();
		String s;
		for (int i = 0; i < numDistinctCompressedCommands; i++){
			s = gzip.generateString(stringSize, charactersPattern);
			try {
				compressedCommands.put(i, gzip.compress(s));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public byte[] getCompressedCmd(){
		Random rand = new Random();	
		return compressedCommands.get(rand.nextInt(numDistinctCompressedCommands));		
	}
	
	public  byte[] compressedCmd(String cmd){
		GzipCompress gzip = new GzipCompress();
		byte[] r=null;
		try {
			r = gzip.compress(cmd);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return r;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String zoo_host = "127.0.0.1:2181";
		String connectInfo;
		int distinct_keys;
		String chars_pattern;

		if (args.length > 1) {
			zoo_host = args[1];
		}
		if (args.length < 1) {
			
			ConfigurationFileManager conf = new ConfigurationFileManager();
			try{
				conf.openFile("setup.txt");
			}catch(Exception e){
				System.err.println("Plese use \"Client\" \"ring ID,node ID[;ring ID,node ID]\"");
				System.err.println("Or configure replica settings in the file setup.txt");
				System.exit(1);
			}
			
			connectInfo = conf.getProperty("connect_map");						
			zoo_host = conf.getProperty("zookeeper_host");
			distinct_keys = Integer.parseInt(conf.getProperty("num_distinct_keys"));
			chars_pattern = conf.getProperty("chars_pattern");
			
			System.out.println("Connect Map "+connectInfo+"; Zookeeper "+zoo_host+"; ");			
			System.out.println("Num. of distinct keys "+distinct_keys+"; Chars Pattern "+chars_pattern);
			
			final Map<Integer,Integer> connectMap = parseConnectMap(connectInfo);
			try {
				final PartitionManager partitions = new PartitionManager(zoo_host);
				partitions.init();
				final Client client = new Client(partitions,connectMap,distinct_keys,chars_pattern);
				Runtime.getRuntime().addShutdownHook(new Thread("ShutdownHook"){
					@Override
					public void run(){
						client.stop();
					}
				});
				client.init();
				client.readStdin();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
			
		} else {
			final Map<Integer,Integer> connectMap = parseConnectMap(args[0]);
			try {
				final PartitionManager partitions = new PartitionManager(zoo_host);
				partitions.init();
				final Client client = new Client(partitions,connectMap);
				Runtime.getRuntime().addShutdownHook(new Thread("ShutdownHook"){
					@Override
					public void run(){
						client.stop();
					}
				});
				client.init();
				client.readStdin();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	public static Map<Integer, Integer> parseConnectMap(String arg) {
		Map<Integer,Integer> connectMap = new HashMap<Integer,Integer>();
		for(String s : arg.split(";")){
			connectMap.put(Integer.valueOf(s.split(",")[0]),Integer.valueOf(s.split(",")[1]));
		}
		return connectMap;
	}

	@Override
	public boolean is_ready(Integer ring, Long instance) {
		return true;
	}
}
