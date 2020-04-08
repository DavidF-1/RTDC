package bitcoin;

import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.tuple.ITuple;
import org.json.simple.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.UUID;

@SuppressWarnings("serial")
public class UTXEsTupleMapper implements EsTupleMapper {

	private SimpleDateFormat dateform = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    @SuppressWarnings("unchecked")
	@Override
    public String getSource(ITuple tuple) {
    	JSONObject document = new JSONObject();
    	
    	Long transaction_timestamp = tuple.getLongByField("transaction_timestamp");
    	String utxdate = dateform.format(transaction_timestamp * 1000);
    	String transaction_hash = tuple.getStringByField("transaction_hash");
    	Double transaction_total_amount = tuple.getDoubleByField("transaction_total_amount");
    	Double transaction_value = tuple.getDoubleByField("transaction_value");  
        	
    	document.put("date", utxdate);
    	document.put("transaction_hash", transaction_hash);
    	document.put("transaction_total_amount", transaction_total_amount);
    	document.put("transaction_value", transaction_value);

    	return document.toString();
    }

    @Override
    public String getIndex(ITuple tuple) {
    		return "utx_index";
    }

    @Override
    public String getType(ITuple tuple) {
    		return "utx_type";
    }

    @Override
    public String getId(ITuple tuple) {
    	String id=UUID.randomUUID().toString();
        return id;
    }

    @Override
    public Map<String, String> getParams(ITuple tuple, Map<String, String> map) {
        map.put("op_type", "create");
        return map;
    }
}