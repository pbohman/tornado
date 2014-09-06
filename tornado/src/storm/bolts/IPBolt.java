package storm.bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;


import org.jnetpcap.nio.JMemory.Type;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.protocol.network.Ip4;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.topology.IRichBolt;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import storm.util.TupleHelpers;



public class IPBolt implements IRichBolt{

	private Ip4 ip;
	private OutputCollector collector;
	private Map<String, Map<Long, int[]>> dstIpStats;
	private BloomFilter<String> observedIPs;
	private static int NUM_FILTER_ITEMS = 8000000;
	private static double FALSE_POS_RATE = 0.15;
	private Map<Long, Integer> newIPRate;
	
	public static byte[] unpack_ip(int bytes) {
		  return new byte[] {
		    (byte)((bytes >>> 24) & 0xff),
		    (byte)((bytes >>> 16) & 0xff),
		    (byte)((bytes >>>  8) & 0xff),
		    (byte)((bytes       ) & 0xff)
		  };
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("ipDstStats", new Fields("dstIP", "interval", "packets", "bytes", "tcp", "udp", "icmp"));
		declarer.declareStream("ipSrcStats", new Fields("timestamp", "value"));
	}

	@SuppressWarnings("serial")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		ip = new Ip4(); 
		dstIpStats = new HashMap<String, Map<Long, int[]>>();
		
		// Unique src IP tracking
		Funnel<String> ipFunnel = new Funnel<String>() {

			@Override
			  public void funnel(String ip, PrimitiveSink into) {
			    into.putString(ip);
			  }

		}; 
		observedIPs = BloomFilter.create(ipFunnel, NUM_FILTER_ITEMS, FALSE_POS_RATE);
		newIPRate = new TreeMap<Long, Integer>();
	}
	

	private void statSrcIP(long sec, String src){
		if(!observedIPs.mightContain(src)){
			Integer rate = newIPRate.get(sec);
			if (rate == null) rate = new Integer(0);
			rate += 1;
			newIPRate.put(sec, rate);
			observedIPs.put(src);
		}
	}
	
	private void statIP(long sec, PcapPacket p, String src) throws UnknownHostException{
		if (p.hasHeader(ip)) {

			int dstInt = ip.destinationToInt();
			String dst = InetAddress.getByAddress(unpack_ip(dstInt)).getHostAddress();
			int protocol = ip.type();
			int len = ip.length();
			
			// Get dstIP intervals
			Map<Long, int[]> intervalStats = (Map<Long, int[]>) dstIpStats.get(dst);
			if(intervalStats==null) intervalStats = new TreeMap<Long, int[]>();
			
			// Update interval stats
			int[] values = intervalStats.get(sec);
			if(values==null) values = new int[5];
			values[0] += 1;
			values[1] += len;
			if(protocol == 6){
				values[2] += len;
			}
			else if(protocol == 17){
				values[3] += len;
			}
			else if (protocol == 1){
				values[4] += len;
			}
			
			intervalStats.put(sec, values);
			dstIpStats.put(dst, intervalStats);
		}
	}
	
	private void dumpDstStats(){
		for (Entry<String, Map<Long, int[]>> entry : dstIpStats.entrySet())
		{
			String ip = entry.getKey();
			Map<Long, int[]> intervals = entry.getValue();
			for (Entry<Long, int[]> interval : intervals.entrySet()){
				Long sec = interval.getKey();
				int[] values = interval.getValue();
				this.collector.emit("ip_dst_stats", new Values(ip, sec, values[0], values[1], values[2], values[3], values[4]));
			}			
		}
		dstIpStats = new HashMap<String, Map<Long, int[]>>();
		return;
	}
	
	private void dumpSrcStats(){
		for (Entry<Long, Integer> entry : newIPRate.entrySet()){
			System.out.println(entry.getKey() + ": " + entry.getValue());
			this.collector.emit("ip_src_stats", new Values(entry.getKey(), entry.getValue()));
		}
		newIPRate = new TreeMap<Long, Integer>();
	}

	@Override
	public void execute(Tuple input) {

		if (TupleHelpers.isTickTuple(input)) {
			dumpDstStats();
			dumpSrcStats();
			//collector.ack(input);
			return;
		}
		
		byte[] pBuffer = input.getBinary(0);
		PcapPacket p = new PcapPacket(Type.POINTER); // Uninitialized
		p.transferStateAndDataFrom(pBuffer);
		long sec = p.getCaptureHeader().seconds();
        String src = input.getString(1);
		
        if (p.hasHeader(ip)) {
            try {
				statIP(sec, p, src);
				statSrcIP(sec, src);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
        }
        collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
