package storm.bolt.deal;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import storm.util.KafkaSendUtils;
import storm.system.SysDefine;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;


public class KafkaSendBolt extends BaseRichBolt {

	private static final long serialVersionUID = 17100009L;
	private OutputCollector collector;
    private static int batchNo=600;
    private long lastExeTime;
    private BlockingQueue<KeyedMessage<byte[], byte[]>> keyedMessageQueue;
    private BlockingQueue<KeyedMessage<byte[], byte[]>> saveMessageQueue;
    
    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        keyedMessageQueue = new LinkedBlockingQueue<KeyedMessage<byte[], byte[]>>();
        saveMessageQueue = new LinkedBlockingQueue<KeyedMessage<byte[], byte[]>>();
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
        		||(tuple.getSourceStreamId().equals(SysDefine.FAULT_STREAM))
        		||(tuple.getSourceStreamId().equals(SysDefine.SYNES_NOTICE))
        		||(tuple.getSourceStreamId().equals(SysDefine.CUS_NOTICE))
        		){
            try {
				addMessage(tuple.getValue(0).toString(), tuple.getValue(1).toString(), 
						tuple.getValue(2).toString(),keyedMessageQueue);
//            realtimeSend(tuple.getSourceStreamId(),keyedMessageQueue);
				batchSend(batchNo, batchNo, true, keyedMessageQueue);
			} catch (Exception e) {
				e.printStackTrace();
			}
            
        } else if (   (tuple.getSourceStreamId().equals(SysDefine.HISTORY)) 
        			||(tuple.getSourceStreamId().equals(SysDefine.SYNC_REALINFO_STORE))
        		){
        	
            try {
				addMessage(tuple.getValue(0).toString(), tuple.getValue(1).toString(), 
						tuple.getValue(2).toString(),saveMessageQueue);
				
				boolean istimecu=false;
				long now = System.currentTimeMillis();
				if (now - lastExeTime >= 20 * 1000){
					lastExeTime=now;
					istimecu=true;
				}
				batchSend(batchNo, batchNo, istimecu, saveMessageQueue);
			} catch (Exception e) {
				e.printStackTrace();
			}
        } 
        
    }
    private void batchSend(int total,int batch,boolean isexe,
    		BlockingQueue<KeyedMessage<byte[], byte[]>> messageQueue){
    	if (messageQueue != null 
    			&& (
    					messageQueue.size() >= total
    					|| (isexe && messageQueue.size() > 0)
    				)
    			) {
        	Producer<byte[], byte[]> producer=KafkaSendUtils.getPoolProducer();
        	
        	List<KeyedMessage<byte[], byte[]>> messages = new LinkedList<KeyedMessage<byte[], byte[]>>();
            KeyedMessage<byte[], byte[]> keyedMessage = messageQueue.poll();
            
            while (keyedMessage != null) {
            	messages.add(keyedMessage);
            	
                if (messages.size()>=batch) {
                	producer=sendMsgs(producer,messages);
				}
                keyedMessage = messageQueue.poll();
            }
            producer=sendMsgs(producer,messages);
            KafkaSendUtils.returnProducerConn(producer);
            messages=null;
        }
    }
    private Producer<byte[], byte[]> sendMsgs(Producer<byte[], byte[]> producer,
    		List<KeyedMessage<byte[], byte[]>> messages){
    	while (messages.size()>0) {
			try {
				producer.send(messages);
				messages.clear();
			} catch (Exception e) {
				e.printStackTrace();
				producer = KafkaSendUtils.getNewProducer();
			} 
		}
		return producer;
    }
    void realtimeSend(String streamId,BlockingQueue<KeyedMessage<byte[], byte[]>> messageQueue){
    	if (messageQueue != null && messageQueue.size() > 0) {
        	Producer<byte[], byte[]> producer=KafkaSendUtils.getPoolProducer();
        	
            KeyedMessage<byte[], byte[]> keyedMessage = messageQueue.poll();
            while (keyedMessage != null) {
            	try {
                    producer.send(keyedMessage);
                    keyedMessage = messageQueue.poll();
                } catch (Exception e) {
                    e.printStackTrace();
                    producer=KafkaSendUtils.getNewProducer();
                }
            }
            
            KafkaSendUtils.returnProducerConn(producer);
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    public void setKeyedMessageQueue(KeyedMessage<byte[], byte[]> keyedMessage,
    		BlockingQueue<KeyedMessage<byte[], byte[]>> messageQueue){
        if (keyedMessage != null){
            try {
            	messageQueue.put(keyedMessage);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void addMessage(String topic,String key,String val,
    		BlockingQueue<KeyedMessage<byte[], byte[]>> messageQueue){
        KeyedMessage<byte[], byte[]> message = null;
        try {
            message = new KeyedMessage<byte[], byte[]>(topic, key.getBytes("UTF-8"), val.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        setKeyedMessageQueue(message,messageQueue);
    }

}
