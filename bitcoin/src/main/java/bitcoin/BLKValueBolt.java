package bitcoin;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class BLKValueBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private Double bpi_price=0.;
	private Long block_timestamp;
	private String block_hash;
	private Double block_value;
	private String block_found_by;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		try {
			if(input.getSourceComponent().equals("bpiBolt")) {
				bpi_price = input.getDoubleByField("bpi_price");
				if (block_hash != null) {
					block_value = bpi_price * 12.5 ;
					
					outputCollector.emit(input,new Values(block_timestamp,block_hash,block_value,block_found_by));				
					block_timestamp = null;
					block_hash = null;
					block_value = 0.;
					block_found_by = null;			
					outputCollector.ack(input);
				}
			} else {	
				block_timestamp = input.getLongByField("block_timestamp");
				block_hash = input.getStringByField("block_hash");	
				block_found_by = (String) input.getStringByField("block_found_by");
				outputCollector.ack(input);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("block_timestamp","block_hash","block_value","block_found_by"));
	}
}