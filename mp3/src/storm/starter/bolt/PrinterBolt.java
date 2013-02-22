package storm.starter.bolt;

import storm.starter.util.TupleHelpers;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class PrinterBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(tuple)) {
			System.out.println();
			return;
		}
        System.out.println("OUTPUT TUPLE:" + tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
    
}
