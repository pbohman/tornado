package storm;

import java.io.IOException;

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

public class Topology {
	
	public static void display_usage(){
		System.out.println("<kestrel server> <kestrel port> <kestrel queue name> " +
				"<mongoHost> <mongoPort> <mongoDbName> <mongoDstCollectionName>" +
				"  <mongoSrcCollectionName> [local?]");
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
    	
        /**
         * Build topology kestrel-q -> decompression -> pcap-splitter -> packets -> *stats -> db
         * pcap parsing and splitting requires the most processing, so create more of those
         */
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
        
       
		// Performance Tuning
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
		conf.setNumWorkers(20);
		conf.setMaxSpoutPending(5000);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,16384);
		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 3600);
		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        
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
        		e.printStackTrace();
        	} catch (InvalidTopologyException e) {
        		e.printStackTrace();
        	}        	
        }
    }
}
