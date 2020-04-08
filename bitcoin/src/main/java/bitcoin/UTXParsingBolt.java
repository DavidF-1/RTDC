package bitcoin;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@SuppressWarnings("serial")
public class UTXParsingBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			JSONParser jsonParser = new JSONParser();
			JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));
			Long transaction_timestamp = (Long)obj.get("transaction_timestamp");
			String transaction_hash = (String)obj.get("transaction_hash");			
			Double transaction_total_amount = (Double)obj.get("transaction_total_amount");

			outputCollector.emit(input, new Values(transaction_timestamp, transaction_hash, transaction_total_amount));
			outputCollector.ack(input);	
			
		} catch (ParseException e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("transaction_timestamp", "transaction_hash", "transaction_total_amount"));
	}
}
