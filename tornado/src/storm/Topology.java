package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import backtype.storm.spout.*;
import storm.bolts.*;
import backtype.storm.StormSubmitter;

import org.jnetpcap.Pcap;  

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class Topology {
	
	private static final int TWO = 2;
	private static final int FIVE = 5;
	private static final StringBuilder errbuf = new StringBuilder();
	private static final String tmpDirPath = "/dev/shm/";
	
	
	public static void unGzip(File infile, File outFile, boolean deleteGzipfileOnSuccess ) throws IOException {
		  GZIPInputStream gin = new GZIPInputStream(new FileInputStream(infile));
		  FileOutputStream fos = new FileOutputStream(outFile);
		  byte[] buf = new byte[100000]; // Buffer size is a matter of taste and application...
		  int len;
		  while ( ( len = gin.read(buf) ) > 0 )
		    fos.write(buf, 0, len);
		  gin.close();
		  fos.close();
		  if ( deleteGzipfileOnSuccess )
		    infile.delete();
	}
	
	/**
	 * Decompresses and reads pcap file. In the future, the data source could be
	 * a kestrel queue that stores the compressed files in memory.
	 * 
	 * @param inputFile
	 * @throws IOException 
	 */
	public static Pcap loadPcap(String inputFileName) throws IOException{
		
		File input = new File(inputFileName);
		File tmpDir = new File(tmpDirPath);
		File outFile = File.createTempFile("tmp", "pcap", tmpDir);
		unGzip(input, outFile, false);
		
		return Pcap.openOffline(outFile.getAbsolutePath(), errbuf);
	}
	
	public static void display_usage(){
		System.out.println("<kestrel server> <kestrel port> <kestrel queue name> <mongoHost> <mongoPort> <mongoDbName> <mongoDstCollectionName>  <mongoSrcCollectionName> [local?]");
	}
	
    public static void main(String[] args) throws IOException {
    	
    	boolean isLocal = false;
    	if(args.length < 8 || args.length > 9){
    		display_usage();
    		return;
    	}
    	else if (args.length == 9){
    		isLocal = true;
    	}
    	
    	
    	KestrelThriftSpout kestrelSpout = new KestrelThriftSpout(args[0], Integer.parseInt(args[1]), args[2]);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("CompPcap", kestrelSpout);        
        builder.setBolt("PcapSplitter",new PcapSplitter(), 7).shuffleGrouping("compPcap");
        builder.setBolt("IPBolt", new IPBolt(), 5).fieldsGrouping("PcapSplitter", "packets", new Fields("srcIP"));
        builder.setBolt("IPBoltDstStats", new IPDstCount(), 1).fieldsGrouping("IPBolt", "ipDstStats", new Fields("dstIP"));
        builder.setBolt("IPDstMongo", new IPMongoBolt(args[3], 
        									Integer.parseInt(args[4]),
        									args[5],
        									args[6]), 
        				1).shuffleGrouping("IPBoltDstStats", "ipDstStatsAgg");
        builder.setBolt("IPSrcMongo", new IPMongoBolt(args[3], 
											Integer.parseInt(args[4]),
											args[5],
											args[7]), 
						1).shuffleGrouping("IPBolt", "ipSrcStats");
        
       
        
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, FIVE);
		conf.setNumWorkers(20);
		conf.setMaxSpoutPending(5000);
		// Performance Tuning
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,16384);
		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 3600);
		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
		
        //conf.setDebug(true);
        
        if(isLocal){
        	LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology("tornado-topology", conf, builder.createTopology());
            Utils.sleep(4500000);
            cluster.shutdown();
        }
        else {
        	try {
        		StormSubmitter.submitTopology("tornado-topology", conf, builder.createTopology());
        	} catch (AlreadyAliveException e) {
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        	} catch (InvalidTopologyException e) {
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        	}
        	
        }

    }
}
