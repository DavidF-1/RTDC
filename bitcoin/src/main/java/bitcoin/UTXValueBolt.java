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
public class UTXValueBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private Double bpi_price = 0.;
	private Long transaction_timestamp;
	private String transaction_hash;
	private Double transaction_total_amount;
	private Double transaction_value;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}
	
	public void execute(Tuple input) {
		try {
			if(input.getSourceComponent().equals("utxBolt")) {
				transaction_timestamp = input.getLongByField("transaction_timestamp");
				transaction_hash = input.getStringByField("transaction_hash");			
				transaction_total_amount = input.getDoubleByField("transaction_total_amount");
				if (bpi_price != 0.00) {
					transaction_value = bpi_price * transaction_total_amount;
					
					outputCollector.emit(input,new Values(transaction_timestamp, transaction_hash, transaction_total_amount, transaction_value));			
					transaction_timestamp = null;
					transaction_hash = null;
					transaction_total_amount = 0.;
					transaction_value = 0.;		
					outputCollector.ack(input);		
				}	
			} 
			else {				
				bpi_price = input.getDoubleByField("bpi_price");
				outputCollector.ack(input);
			}
		} catch (Exception e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transaction_timestamp", "transaction_hash", "transaction_total_amount", "transaction_value"));
	}
}
