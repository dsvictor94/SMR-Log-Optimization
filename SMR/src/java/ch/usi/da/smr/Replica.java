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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.Util;
import ch.usi.da.smr.Replica;
import ch.usi.da.smr.log.LoggerFactory;
import ch.usi.da.smr.log.LoggerInterface;
import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.misc.ConfigurationFileManager;
import ch.usi.da.smr.misc.GzipCompress;
import ch.usi.da.smr.recovery.CheckpointType;
import ch.usi.da.smr.recovery.DfsRecovery;
import ch.usi.da.smr.recovery.FTPRecovery;
import ch.usi.da.smr.recovery.HttpRecovery;
import ch.usi.da.smr.recovery.RecoveryInterface;
import ch.usi.da.smr.recovery.SnapshotWriter;
import ch.usi.da.smr.statetransfer.StateTransferFactory;
import ch.usi.da.smr.statetransfer.StateTransferInterface;
import ch.usi.da.smr.transport.ABListener;
import ch.usi.da.smr.transport.Receiver;
import ch.usi.da.smr.transport.UDPSender;

/**
 * Name: Replica<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Replica implements Receiver {
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

	private final static Logger logger = Logger.getLogger(Replica.class);
	
	public final int nodeID;

	public final String token;
	
	private final PartitionManager partitions;
	
	private volatile int min_token;
	
	private volatile int max_token; // if min_token > max_token : replica serves whole key space

	private final UDPSender udp;

	private final ABListener ab;
	
	private volatile SortedMap<String,byte[]> db;
		
	private volatile Map<Integer,Long> exec_instance = new HashMap<Integer,Long>();
	
	private long exec_cmd = 0;
	
	private int snapshot_modulo = 0; // disabled
	
	private final boolean use_thrift = false;
	
	RecoveryInterface stable_storage;
	
	private volatile boolean recovery = false;
	
	private volatile boolean active_snapshot = false;
	
//	private Thread recovery = null;

	private boolean compressedCmds = true;
	private GzipCompress gzip = null;
	
	// for throughput statistics
	private int measure[] = new int[2];
	private Boolean round = false;		// this attribute alternates in order to keep always the last two more recent measurements  
	private float timestamp;
	private int cmdCount = 0;	
	
	private int printThroughputInterval = 1;	// interval in seconds - 0 disabled
	private boolean debug_checkpoint = true;

	private CheckpointType checkpointType = CheckpointType.NONE;
	
	private String serverFTP = "192.168.3.91";
	private int portFTP = 2121;
	
	private LoggerInterface replicaLogger;

	private StateTransferInterface stateTransfer;
	
	public Replica(String token, int ringID, int nodeID, int snapshot_modulo, String zoo_host) throws Exception {
		
		this.nodeID = nodeID;
		this.token = token;
		this.snapshot_modulo = snapshot_modulo;
		this.partitions = new PartitionManager(zoo_host);
		final InetAddress ip = Util.getHostAddress();
		partitions.init();
		setPartition(partitions.register(nodeID, ringID, ip, token));
		udp = new UDPSender();
		if(use_thrift){
			ab = partitions.getThriftABListener(ringID,nodeID);
		}else{
			ab = partitions.getRawABListener(ringID,nodeID);
		}
		db = new  TreeMap<String,byte[]>();
		//stable_storage = new HttpRecovery(partitions);
		
		if(checkpointType == CheckpointType.TCP){
			//System.out.println("Set up checkpoint using TCP");
			stable_storage = new FTPRecovery(nodeID,token,"/tmp/smr",partitions,serverFTP,portFTP);
		}
		else{
			//System.out.println("Set up checkpoint using File System");
			stable_storage = new DfsRecovery(nodeID,token,"/tmp/smr",partitions);
		}
		
		this.replicaLogger = LoggerFactory.getLogger();
		this.replicaLogger.initialize(ab);
		this.stateTransfer = StateTransferFactory.getStateTransfer();
		this.stateTransfer.init(this.replicaLogger);

		if(compressedCmds)
			gzip = new GzipCompress();
		
	}

	public Replica(String token, int ringID, int nodeID, int snapshot_modulo, String zoo_host, int printOutputInterval,boolean debugCheckpoint) throws Exception {
		
		this.printThroughputInterval = printOutputInterval;		
		this.debug_checkpoint = debugCheckpoint;
		
		this.nodeID = nodeID;
		this.token = token;
		this.snapshot_modulo = snapshot_modulo;
		this.partitions = new PartitionManager(zoo_host);
		final InetAddress ip = Util.getHostAddress();
		partitions.init();
		setPartition(partitions.register(nodeID, ringID, ip, token));
		udp = new UDPSender();
		if(use_thrift){
			ab = partitions.getThriftABListener(ringID,nodeID);
		}else{
			ab = partitions.getRawABListener(ringID,nodeID);
		}
		db = new  TreeMap<String,byte[]>();
		//stable_storage = new HttpRecovery(partitions);

		if(checkpointType == CheckpointType.TCP){
			//System.out.println("Set up checkpoint using TCP");
			stable_storage = new FTPRecovery(nodeID,token,"/tmp/smr",partitions,serverFTP,portFTP);
		}
		else{
			//System.out.println("Set up checkpoint using File System");
			stable_storage = new DfsRecovery(nodeID,token,"/tmp/smr",partitions);
		}
		
		this.replicaLogger = LoggerFactory.getLogger();
		this.replicaLogger.initialize(ab);
		this.stateTransfer = StateTransferFactory.getStateTransfer();
		this.stateTransfer.init(this.replicaLogger);
		
		if(compressedCmds)
			gzip = new GzipCompress();
		
		// TODO: Just create a new file or remove a previous one if it exists.
		if(debug_checkpoint){			
			try {
				FileOutputStream debugFile = new FileOutputStream("./cp_duration.txt");				
				debugFile.write("#timestamp  cp duration  num_keys_stored\n".getBytes());
				debugFile.flush();
				debugFile.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	
	public void setPartition(Partition partition){
		logger.info("Replica update partition " + partition);
		min_token = partition.getLow();
		max_token = partition.getHigh();
	}

	public void start(){
		partitions.registerPartitionChangeNotifier(this);
		// install old state
		exec_instance = load();
		// start listening
		ab.registerReceiver(this);
		if(min_token > max_token){
			logger.info("Replica start serving partition " + token + ": whole key space");
		}else{
			logger.info("Replica start serving partition " + token + ": " + min_token + "->" + max_token);			
		}
		Thread t = new Thread((Runnable) ab);
		t.setName("ABListener");
		t.start();
		
		
		Thread t_stat = new Thread("Stats"){
			@Override
			public void run(){
				//int delay = 5;
				//int duration = 120;

				//int time=0; 
				//while(time < duration){
				
				if(printThroughputInterval > 0){
					while(true){					
						try {		
							//Thread.sleep(1000*(delay+duration+10));
							//Thread.sleep(1000*(delay));
							Thread.sleep(1000*printThroughputInterval);
							//long th=0;
							//for(int i = 0; i < numWorkingThreads;i++)
							//	th+= throughputOutput[i];
							//System.out.println("throughput "+th);
							//System.out.println("parallelizer throughput: "+parallelizer.getThroughput());
							System.out.printf("%d  %.5f %d\n",System.nanoTime(),getThroughput(), replicaLogger.size());
						} catch (InterruptedException e) {				
							e.printStackTrace();
						}
					}
				}
			}
		}; 
		t_stat.start();
	}

	public void close(){
		ab.close();
		stable_storage.close();
		stateTransfer.close();
		//partitions.deregister(nodeID,token);
	}

	private Message execute(Message m) {
		List<Command> cmds = new ArrayList<Command>();

		synchronized(db){
			byte[] data = null;
			for(Command c : m.getCommands()){
		    	switch(c.getType()){
		    	case PUT:
		    		if(compressedCmds){
		    			try {
							data = gzip.decompress(c.getValue()).getBytes();
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}		
		    		}
		    		else 
		    			data = c.getValue();
		    		db.put(c.getKey(),data);
		    		if(db.containsKey(c.getKey())){
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"OK".getBytes());
		    			cmds.add(cmd);
		    		}else{
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"FAIL".getBytes());
		    			cmds.add(cmd);
		    		}
		    		break;
				case DELETE:
		    		if(db.remove(c.getKey()) != null){
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"OK".getBytes());
		    			cmds.add(cmd);
		    		}else{
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"FAIL".getBytes());
		    			cmds.add(cmd);
		    		}
					break;
				case GET:
		    		if(compressedCmds){
		    			try {
		    				byte tmp[]= db.get(c.getKey());
		    				if(tmp != null)
		    					data = gzip.compress(new String(tmp));
		    				else
		    					data = null;
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}  			
		    		}
		    		else 
		    			data = db.get(c.getKey());
					if(data != null){
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),data);
		    			cmds.add(cmd);
					}else{
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),null);
		    			cmds.add(cmd);
					}
					break;
				case GETRANGE: // key range (token range not implemented)
					/* Inspired by the Cassandra API:
					The semantics of start keys and tokens are slightly different. 
					Keys are start-inclusive; tokens are start-exclusive. Token 
					ranges may also wrap -- that is, the end token may be less than 
					the start one. Thus, a range from keyX to keyX is a one-element 
					range, but a range from tokenY to tokenY is the full ring (one 
					exception is if keyX is mapped to the minimum token, then the 
					range from keyX to keyX is the full ring).
					Attribute	Description
					start_key	The first key in the inclusive KeyRange.
					end_key		The last key in the inclusive KeyRange.
					start_token	The first token in the exclusive KeyRange.
					end_token	The last token in the exclusive KeyRange.
					count		The total number of keys to permit in the KeyRange.
					 */
					String start_key = c.getKey();
					String end_key = new String(c.getValue());
					int count = c.getCount();
					logger.debug("getrange " + start_key + " -> " + end_key + " (" + MurmurHash.hash32(start_key) + "->" + MurmurHash.hash32(end_key) + ")");
					//logger.debug("tailMap:" + db.tailMap(start_key).keySet() + " count:" + count);
					int msg = 0;
					int msg_size = 0;
					for(Entry<String,byte[]> e : db.tailMap(start_key).entrySet()){
						if(msg >= count || (!end_key.isEmpty() && e.getKey().compareTo(end_key) > 0)){ break; }
						if(msg_size >= 50000){ break; } // send by UDP						
			    		Command cmd = new Command(c.getID(),CommandType.RESPONSE,e.getKey(),e.getValue());
			    		msg_size += e.getValue().length;
			    		cmds.add(cmd);
			    		msg++;
					}
					if(msg == 0){
			    		Command cmd = new Command(c.getID(),CommandType.RESPONSE,"",null);
			    		cmds.add(cmd);						
					}
					break;
				default:
					System.err.println("Receive RESPONSE as Command!"); break;
		    	}
			}
		}
		
		int msg_id = MurmurHash.hash32(m.getInstnce() + "-" + token);
		return new Message(msg_id,token,m.getFrom(), cmds);
	}

	@Override
	public void receive(Message m) {
		logger.debug("Replica received ring " + m.getRing() + " instnace " + m.getInstnce() + " (" + m + ")");
		
		while (m.getInstnce()-1 > exec_instance.get(m.getRing())) {
			logger.debug("Replica start recovery: " + exec_instance.get(m.getRing()) + " to " + (m.getInstnce()-1));
			exec_instance = load();
			
			logger.debug("Replica start recovery from log: " + exec_instance.get(m.getRing()) + " to " + (m.getInstnce()-1));

			for(Message lm : this.stateTransfer.restore(m.getRing(), exec_instance.get(m.getRing()), m.getInstnce()-1)) {
				execute(lm);
				exec_instance.put(lm.getRing(), lm.getInstnce());
			}
		}

		// skip already executed commands
		if (m.getInstnce() <= exec_instance.get(m.getRing())) {
			return;
		} else if(m.isSkip() ){ // skip skip-instances
			exec_instance.put(m.getRing(),m.getInstnce());
			return;
		}

		// send to logger
		this.replicaLogger.store(m);

		// TODO: adicionar regra para commit
		this.replicaLogger.commit(m.getRing());

		// write snapshot
		exec_cmd++;
		if(snapshot_modulo > 0 && exec_cmd % snapshot_modulo == 0){
			async_checkpoint(); 
		}

		Message msg = execute(m);
		exec_instance.put(m.getRing(),m.getInstnce());
		//logger.debug("Send UDP: " + msg);
		udp.send(msg);
		
		cmdCount += msg.getCommands().size();
	}

	public Map<Integer,Long> load(){
		try{
			return stable_storage.installState(token,db);
		}catch(Exception e){
			if(!exec_instance.isEmpty()){
				return exec_instance;
			}else{ // init to 0
				Map<Integer,Long> instances = new HashMap<Integer,Long>();
				instances.put(partitions.getGlobalRing(),0L);
				for(Partition p : partitions.getPartitions()){
					instances.put(p.getRing(),0L);
				}
				return instances;
			}
		}
	}
	
	public boolean sync_checkpoint(){
		if(stable_storage.storeState(exec_instance,db)){
			try {
				for(Entry<Integer, Long> e : exec_instance.entrySet()){
					ab.safe(e.getKey(),e.getValue());
				}
				logger.debug("Replica checkpointed up to instance " + exec_instance);
				//logger.info("Replica checkpointed up to instance " + exec_instance);
				return true;
			} catch (Exception e) {
				logger.error(e);
			}
		}
		return false;
	}

	public boolean async_checkpoint(){
		if(!active_snapshot){
			active_snapshot = true;
			// shallow copy
			Map<Integer,Long> old_exec_instance = new HashMap<Integer,Long>(exec_instance);
			SortedMap<String,byte[]> old_db = new TreeMap<String,byte[]>(db);
			// deep copy
			/* Map<Integer,Long> old_exec_instance = new HashMap<Integer,Long>();
			for(Entry<Integer,Long> e : exec_instance.entrySet()){
				old_exec_instance.put(new Integer(e.getKey()),new Long(e.getValue()));
			}
			SortedMap<String,byte[]> old_db = new TreeMap<String,byte[]>();
			for(Entry<String,byte[]> e : db.entrySet()){
				old_db.put(new String(e.getKey()),Arrays.copyOf(e.getValue(),e.getValue().length));
			}
			old_db.putAll(db); */
			Thread t = new Thread(new SnapshotWriter(this,old_exec_instance,old_db,stable_storage,ab, replicaLogger));
			t.start();
		}else{
			logger.debug("Async checkpoint supressed since other active!");
			//logger.info("Async checkpoint supressed since other active!");
		}
		return true;
	}
	
	/*
	public boolean async_checkpoint(){
		Thread t = new Thread(new SnapshotWriter(exec_instance,db,stable_storage,ab));
		t.start();
		// create "copy-on-write" (shallow) maps
		exec_instance = new HashMap<Integer,Long>(exec_instance);
		db = new TreeMap<String,byte[]>(db);		
		return true;
	}
	 */
	
	/**
	 * Do not accept commands until you know you have recovered!
	 * 
	 * The commands are queued in the learner itself.
	 * 
	 */
	@Override
	public boolean is_ready(Integer ring, Long instance) {
		if(instance <= exec_instance.get(ring)+1){
			if(recovery == true){
				recovery = false;
				logger.debug("Recovery set false.");
				//logger.info("Recovery set false.");
			}
			return true;
		}
		if(recovery == false){
			recovery = true;
			Thread t = new Thread(){
				@Override
				public void run() {
					logger.debug("Replica starts recovery thread.");
					//logger.info("Replica starts recovery thread.");
					while(getRecovery()){
						exec_instance = load();
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							break;
						}
					}
					logger.debug("Recovery thread stopped.");
					//logger.info("Recovery thread stopped.");
				}
			};
			t.setName("Recovery");
			t.start();
		}
		return false;
	}
	
	public void setActiveSnapshot(boolean b){
		active_snapshot = b;
	}
	
	public boolean getRecovery(){
		return recovery;
	}

	public static boolean newerState(Map<Integer, Long> nstate, Map<Integer, Long> state) {
		for(Entry<Integer, Long> e : state.entrySet()){
			long i = e.getValue();
			if(i > 0){
				long ni = nstate.get(e.getKey());
				if(ni > i){
					return true;
				}
			}
		}
		return false;
	}
	
	public float getThroughput(){		
		int sum=0;		
		float previousTime = timestamp;

		timestamp = System.nanoTime();
		
		if(round)			
			measure[0] = cmdCount; 
		else
			measure[1] = cmdCount;
		round = !round;
		
		return Math.abs(measure[1] - measure[0]) / ((timestamp - previousTime) / 1000000000);
	}
	
	public boolean isDebugCheckpointEnabled(){
		return debug_checkpoint;
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String zoo_host = "127.0.0.1:2181";
		int snapshot = 0;
		
		int nodeID;
		int ringID;
		String token;
		boolean debug_checkpoint = false;
		
		if (args.length > 2) {
			zoo_host = args[2];
		}
		if (args.length > 1) {
			snapshot = Integer.parseInt(args[1]);
		}
		if (args.length < 1) {
			
			ConfigurationFileManager conf = new ConfigurationFileManager();
			try{
				conf.openFile("setup.txt");
			}catch(Exception e){
				System.err.println("Plese use \"Replica\" \"ringID,nodeID,Token\" [snapshot_modulo] [zookeeper host]");
				System.err.println("Or configure replica settings in the file setup.txt");
				System.exit(1);
			}	
			
				nodeID = Integer.parseInt(conf.getProperty("node_id"));
				ringID = Integer.parseInt(conf.getProperty("ring_id"));
				token = conf.getProperty("token");
				 	
				zoo_host = conf.getProperty("zookeeper_host");
				snapshot = Integer.parseInt(conf.getProperty("checkpoint_interval"));
				debug_checkpoint = Boolean.parseBoolean(conf.getProperty("debug_checkpoint"));
				
				int printInterval = Integer.parseInt(conf.getProperty("output_interval"));
				
				System.out.println("Node "+nodeID+"; "+"Ring "+ringID+"; "+"token "+token+"; ");
				System.out.println("Zookeeper "+zoo_host+"; ");
				System.out.println("Checkpoint interval "+snapshot+"; "+"Print stats interval "+printInterval);
				System.out.println("Debug Checkpoint "+debug_checkpoint);
				
				try {
					final Replica replica = new Replica(token,ringID,nodeID,snapshot,zoo_host,printInterval,debug_checkpoint);
					Runtime.getRuntime().addShutdownHook(new Thread("ShutdownHook"){
						@Override
						public void run(){
							replica.close();
						}
					});
					replica.start();
					BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
					in.readLine();
					replica.close();
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
		} else {
			String[] arg = args[0].split(",");
			nodeID = Integer.parseInt(arg[1]);
			ringID = Integer.parseInt(arg[0]);
			token = arg[2];
			try {
				final Replica replica = new Replica(token,ringID,nodeID,snapshot,zoo_host);
				Runtime.getRuntime().addShutdownHook(new Thread("ShutdownHook"){
					@Override
					public void run(){
						replica.close();
					}
				});
				replica.start();
				BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
				in.readLine();
				replica.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

}
