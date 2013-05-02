package storm.bolts;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import storm.util.TupleHelpers;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class IPDstCount extends BaseBasicBolt{

	private Map<String, Map<Long, int[]>> dstIpStats = new HashMap<String, Map<Long, int[]>>();	
	public void emit(BasicOutputCollector collector){
		for (Entry<String, Map<Long, int[]>> entry : dstIpStats.entrySet())
		{
			String ip = entry.getKey();
			Map<Long, int[]> intervals = entry.getValue();
			for (Entry<Long, int[]> interval : intervals.entrySet()){
				Long sec = interval.getKey();
				int[] values = interval.getValue();
				
				if(values[0] > 5){
					System.out.println("dst_ip_stats_agg" + ip + 
									" interval " + sec + 
									" packets " + values[0] + 
									" bytes " + values[1] + 
									" tcp " + values[2] +
									" udp " + values[3] +
									" icmp " + values[4]);
				
				
					collector.emit("ipDstStatsAgg",  new Values(ip, sec, new Double(values[0]), values[1], values[2], values[3], values[4]));
				}
			}
		}
		
		dstIpStats = new HashMap<String, Map<Long, int[]>>();
		return;
	}
	
	@Override 
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		if (TupleHelpers.isTickTuple(tuple)) {
			emit(collector);
			return;
		}
		
		String dst = tuple.getString(0);
		Long sec = tuple.getLong(1);
		int packets = tuple.getInteger(2);
		int bytes = tuple.getInteger(3);
		int tcp = tuple.getInteger(4);
		int udp = tuple.getInteger(5);
		int icmp = tuple.getInteger(6);
		
		// Get dstIP intervals
		Map<Long, int[]> intervalStats = (Map<Long, int[]>) dstIpStats.get(dst);
		if(intervalStats==null) intervalStats = new TreeMap<Long, int[]>();
		
		// Get values for this time
		// values(pkt, bytes, bytes-tcp, bytes-udp, bytes-icmp)
		int[] values = intervalStats.get(sec);
		if(values==null) values = new int[5];
		values[0] += packets;
		values[1] += bytes;
		values[2] += tcp;
		values[3] += udp;
		values[4] += icmp;

		
		intervalStats.put(sec, values);
		dstIpStats.put(dst, intervalStats);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("ipDstStatsAgg", new Fields("hostname", "timestamp", "value", "bytes", "tcp", "udp", "icmp"));
		
	}

}
