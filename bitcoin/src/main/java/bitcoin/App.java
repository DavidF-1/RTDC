package bitcoin;

import org.apache.http.Header;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.elasticsearch.bolt.EsIndexBolt;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Bitcoin RealTime Data Process
 */
public class App {
    public static void main (String[] args) throws InvalidTopologyException, AuthorizationException,
    AlreadyAliveException, Exception {

        EsConfig esConfig = new EsConfig("http://localhost:9200");
        Header[] headers = {new BasicHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())};
        esConfig.withDefaultHeaders(headers);
        TopologyBuilder builder = new TopologyBuilder();
        /* KAFKA SPOUT CONFIG */
		KafkaSpoutConfig.Builder<String, String> spoutBPIConfigBuilder = KafkaSpoutConfig.builder("localhost:9092", "bpi")
				.setProp("group.id", "bpiGroupId")
				.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
				.setFirstPollOffsetStrategy(KafkaSpoutConfig.DEFAULT_FIRST_POLL_OFFSET_STRATEGY);
    	KafkaSpoutConfig<String, String> spoutBPIConfig = spoutBPIConfigBuilder.build();

        KafkaSpoutConfig.Builder<String, String> spoutUTXConfigBuilder = KafkaSpoutConfig.builder("localhost:9092", "utx")
        		.setProp("group.id", "utxGroupId")
        		.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
				.setFirstPollOffsetStrategy(KafkaSpoutConfig.DEFAULT_FIRST_POLL_OFFSET_STRATEGY);
        KafkaSpoutConfig<String, String> spoutUTXConfig = spoutUTXConfigBuilder.build();

        KafkaSpoutConfig.Builder<String, String> spoutBLKConfigBuilder = KafkaSpoutConfig.builder("localhost:9092", "blk")
        		.setProp("group.id", "blkGroupId")
        		.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
				.setFirstPollOffsetStrategy(KafkaSpoutConfig.DEFAULT_FIRST_POLL_OFFSET_STRATEGY);
        KafkaSpoutConfig<String, String> spoutBLKConfig = spoutBLKConfigBuilder.build();
        
        /* KAFKA SPOUT */
        builder.setSpout("bpiSpout", new KafkaSpout<>(spoutBPIConfig));
        builder.setSpout("utxSpout", new KafkaSpout<>(spoutUTXConfig));
        builder.setSpout("blkSpout", new KafkaSpout<>(spoutBLKConfig));
        
        /* BOLT FROM SPOUT */
        builder.setBolt("bpiBolt", new BPIParsingBolt(), 2).shuffleGrouping("bpiSpout");
        builder.setBolt("utxBolt", new UTXParsingBolt(), 4).shuffleGrouping("utxSpout");
        builder.setBolt("blkBolt", new BLKParsingBolt(), 2).shuffleGrouping("blkSpout");
        
        /* BOLT APPLICATION PROCESS */
        builder.setBolt("blkValueBolt", new BLKValueBolt(), 2).allGrouping("bpiBolt").shuffleGrouping("blkBolt");
        builder.setBolt("utxValueBolt", new UTXValueBolt(), 2).allGrouping("bpiBolt").shuffleGrouping("utxBolt");
        
        /* ES BOLT */       
        EsTupleMapper bpiEsTupleMapper = new BPIEsTupleMapper();
        EsIndexBolt bpiEsIndexBolt = new EsIndexBolt(esConfig, bpiEsTupleMapper);
        builder.setBolt("bpiEsBolt",  bpiEsIndexBolt).shuffleGrouping("bpiBolt");
        EsTupleMapper blkEsTupleMapper = new BLKEsTupleMapper();
        EsIndexBolt blkEsIndexBolt = new EsIndexBolt(esConfig, blkEsTupleMapper);
        builder.setBolt("blkEsBolt",  blkEsIndexBolt).shuffleGrouping("blkValueBolt");
        EsTupleMapper utxEsTupleMapper = new UTXEsTupleMapper();
        EsIndexBolt utxEsIndexBolt = new EsIndexBolt(esConfig, utxEsTupleMapper);
        builder.setBolt("utxEsBolt",  utxEsIndexBolt).shuffleGrouping("utxValueBolt");

        StormTopology topology = builder.createTopology();

        Config config = new Config();
        config.setMessageTimeoutSecs(60*30);
        config.setNumWorkers(4);

        if (args.length > 0 && args[0].equals("remote")) {
            StormSubmitter.submitTopology("bitcoin", config, topology);
        } else {
            @SuppressWarnings("resource")
			LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("bitcoin", config, topology);
        }
    }
}
    