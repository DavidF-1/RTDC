package bitcoin;

import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.tuple.ITuple;
import org.json.simple.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.UUID;

@SuppressWarnings("serial")
public class BLKEsTupleMapper implements EsTupleMapper {

	private SimpleDateFormat dateform = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    @SuppressWarnings("unchecked")
	@Override
    public String getSource(ITuple tuple) {
    	JSONObject document = new JSONObject();	
    	
		Long block_timestamp = tuple.getLongByField("block_timestamp");
		String blkdate = dateform.format(block_timestamp * 1000);
    	String block_hash = tuple.getStringByField("block_hash");
    	String block_found_by = tuple.getStringByField("block_found_by");
    	Double block_value = tuple.getDoubleByField("block_value");
    	
    	document.put("date",blkdate);
    	document.put("block_hash", block_hash);
    	document.put("block_found_by", block_found_by);
    	document.put("block_value", block_value);
    	
    	return document.toString();
    }

    @Override
    public String getIndex(ITuple tuple) {
    		return "blk_index";
    }

    @Override
    public String getType(ITuple tuple) {
    		return "blk_type";
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