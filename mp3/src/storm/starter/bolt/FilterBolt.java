package storm.starter.bolt;

import storm.starter.util.TupleHelpers;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterBolt extends BaseBasicBolt{

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(input)) {
			return;
		}
		
		if(input.getInteger(1) > 4){
			if (input.size() >= 4){
				collector.emit(new Values(input.getInteger(0), input.getInteger(1), input.getInteger(2), input.getString(3)));
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "retweets", "likes", "geo_location"));
	}

}
