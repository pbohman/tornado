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
import backtype.storm.tuple.Values;

public class TotalingBolt extends BaseBasicBolt {
        private Map<String, Integer> counts = new HashMap<String, Integer>();
        private int total = 0;
        
        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
			if (TupleHelpers.isTickTuple(input)) {
				emitRankings(collector);
				return;
			}
			
            String region = input.getString(0);
            Integer count = counts.get(region);
            if(count==null) count = 0;
            count = count + input.getInteger(1);
            total = total + input.getInteger(1);
            counts.put(region, count);
            
            //System.out.println("DEBUG TUPLE:" + input + " region:" + region + " count:" + count);            
        }
        
        private void emitRankings(BasicOutputCollector collector){
                Iterator it = counts.entrySet().iterator();
                while (it.hasNext()) {
                	Map.Entry pairs = (Map.Entry)it.next();
                    int percentage = (((Integer) pairs.getValue() * 100) / total);
                    collector.emit(new Values((String) pairs.getKey(), percentage + "%"));
                }
        }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("region", "percentage"));
    }

}
