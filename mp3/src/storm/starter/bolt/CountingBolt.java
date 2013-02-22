package storm.starter.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import storm.starter.util.TupleHelpers;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;



public class CountingBolt extends BaseBasicBolt {
	private Map<String, Integer> counts = new HashMap<String, Integer>();
	private int total = 0;
	private static int TWO_SECONDS = 2;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
        if (TupleHelpers.isTickTuple(input)) {
            emitRankings(collector);
        }
        else {
            String region = input.getString(3);
            Integer count = counts.get(region);
            if(count==null) count = 0;
            count = count + input.getInteger(1) + input.getInteger(2);
            total = total + input.getInteger(1) + input.getInteger(2);
            counts.put(region, count);
        }
	}
	
	private void emitRankings(BasicOutputCollector collector){
		Iterator it = counts.entrySet().iterator();
		while (it.hasNext()) {
			
			Map.Entry pairs = (Map.Entry)it.next();
//			collector.emit(new Values((String) pairs.getKey(), (float) pairs.getValue()/total) * 100.0);

		}
	}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("region", "percentage"));
    }

}
