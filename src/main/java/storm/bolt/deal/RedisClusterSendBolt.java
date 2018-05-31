package storm.bolt.deal;

import storm.system.SysDefine;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;


public class RedisClusterSendBolt extends BaseRichBolt {

	private static final long serialVersionUID = 17100009L;
	private OutputCollector collector;
    private static int batchNo=600;
    private long lastExeTime;
    private BlockingQueue<Object> saveMessageQueue;
    
    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        saveMessageQueue = new LinkedBlockingQueue<Object>();
        lastExeTime=System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        if (
        		(tuple.getSourceStreamId().equals(SysDefine.TEST_VEH_ACK))
        		||(tuple.getSourceStreamId().equals(SysDefine.VEH_ALARM))
        		||(tuple.getSourceStreamId().equals(SysDefine.VEH_ALARM_REALINFO_STORE))
        		||(tuple.getSourceStreamId().equals(SysDefine.FENCE_ALARM))
        		||(tuple.getSourceStreamId().equals(SysDefine.YAACTION_NOTICE))
        		){
        	
            
        } else if (   (tuple.getSourceStreamId().equals(SysDefine.HISTORY)) 
        			||(tuple.getSourceStreamId().equals(SysDefine.SYNC_REALINFO_STORE))
        		){
        	
        } 
        
    }
    void realtimeSend(String streamId,BlockingQueue<Object> messageQueue){
    	if (messageQueue != null && messageQueue.size() > 0) {
    		
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

}
