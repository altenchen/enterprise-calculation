package storm.kafka;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class KafkaConfig {
	private String topic;
    private String zkRoot;
    private String spoutId;
    private BrokerHosts brokerHosts;
    private SpoutConfig spoutConfig;
    private int zkPort;
    private String[] zkServers;

    public KafkaConfig(String topic, String zkRoot, String spoutId, String hosts, Scheme scheme) {
        this.topic = topic;
        this.zkRoot = zkRoot;
        this.spoutId = spoutId;
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(zkRoot) || StringUtils.isBlank(spoutId)){
            ;
        }else {
            this.brokerHosts = new ZkHosts(hosts);
            spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
            spoutConfig.scheme = new SchemeAsMultiScheme(scheme);
        }
    }

    public void setZKConfig(int zkPort, String[] zkServers){
        if (spoutConfig!=null || (zkServers!=null && zkServers.length>0)){
            this.zkPort = zkPort;
            this.zkServers = zkServers;
            spoutConfig.zkPort = zkPort;
            spoutConfig.zkServers = Arrays.asList(zkServers);
        }else {
            spoutConfig = null;
        }
    }
    
    public void setOutputStreamId(String streamId){
    	spoutConfig.outputStreamId = streamId;
    }

    public SpoutConfig getSpoutConfig(){
        return spoutConfig;
    }

	public String getTopic() {
		return topic;
	}

	public String getZkRoot() {
		return zkRoot;
	}

	public String getSpoutId() {
		return spoutId;
	}

	public BrokerHosts getBrokerHosts() {
		return brokerHosts;
	}

	public int getZkPort() {
		return zkPort;
	}

	public String[] getZkServers() {
		return zkServers;
	}
}
