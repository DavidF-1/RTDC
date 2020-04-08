package bitcoin;

import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.tuple.ITuple;
import org.json.simple.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.UUID;

@SuppressWarnings("serial")
public class BPIEsTupleMapper implements EsTupleMapper {
	
	private SimpleDateFormat dateform = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
    @SuppressWarnings("unchecked")
	@Override
    public String getSource(ITuple tuple) {
    	JSONObject document = new JSONObject();
    	
    	Long bpi_time = tuple.getLongByField("bpi_time");
		String bpidate = dateform.format(bpi_time * 1000);
    	Double bpi_price = tuple.getDoubleByField("bpi_price");
    	
    	document.put("date", bpidate);
    	document.put("bpi_price", bpi_price);
    	return document.toString();
    }

    @Override
    public String getIndex(ITuple tuple) {
    	return "bpi_index";
    }

    @Override
    public String getType(ITuple tuple) {
    	return "bpi_type";
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