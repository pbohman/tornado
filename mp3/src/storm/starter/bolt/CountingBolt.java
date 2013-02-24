package storm.starter.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import storm.starter.util.TupleHelpers;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.topology.base.BaseRichBolt;

public class CountingBolt extends BaseRichBolt {
	private Map<String, Integer> counts = new HashMap<String, Integer>();
	
	private OutputCollector _collector;
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	    _collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
	        
		if (TupleHelpers.isTickTuple(input)) {
			emitCounts(_collector);
			
		}
		else {
			String region = input.getString(3);
			Integer count = counts.get(region);
			if(count==null) count = 0;
			count = count + input.getInteger(1) + input.getInteger(2);
			counts.put(region, count);
			
		}
		_collector.ack(input);
	}
	
	private void emitCounts(OutputCollector collector){
	        Iterator it = counts.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry)it.next();
				collector.emit(new Values((String) pairs.getKey(),  pairs.getValue()));
				it.remove();
			}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("region", "count"));
	}

}
