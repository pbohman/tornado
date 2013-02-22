package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class FilterBolt extends BaseBasicBolt{

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		if(input.getInteger(1) > 4){
			collector.emit(input.getValues());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "retweets", "likes", "geo_location"));
	}

}
