package storm.bolt.deal.norm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import storm.handler.fault.ExamineService;
import storm.system.DataKey;
import storm.system.SysDefine;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
@SuppressWarnings("all")
public class FaultBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1700001L;

    private OutputCollector collector;

    /**
     * 故障规则检查服务, 来自MySQL数据库的故障规则
     */
    private ExamineService service;
    private static String faultTopic;
    private long lastExeTime;
    private long flushtime;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        faultTopic = stormConf.get("kafka.topic.customfault").toString();
        flushtime=1000*Long.parseLong(stormConf.get(SysDefine.DB_CACHE_FLUSH_TIME_SECOND).toString());
        service = new ExamineService();
        System.out.println("fault rule size----------------->");
        lastExeTime=System.currentTimeMillis();
    }
    @Override
    public void execute(Tuple tuple) {
        long now = System.currentTimeMillis();
        // 超过指定间隔, 则重新初始化检查规则
        if (now - lastExeTime >= flushtime){
            lastExeTime = now;
            service.reflushRules();
        }
        if(tuple.getSourceStreamId().equals(SysDefine.FAULT_GROUP)){
            String vid = tuple.getString(0);
            Map<String, String> alarmMsg = (TreeMap<String, String>) tuple.getValue(1);
            if (null == alarmMsg.get(DataKey.VEHICLE_ID))
                alarmMsg.put(DataKey.VEHICLE_ID, vid);

            //fault result
            List<String>resuts = service.handler(alarmMsg);
            if(null != resuts && resuts.size()>0)
                for (String json : resuts) {
                    if(!isNullOrEmpty(json)){
                        //kafka存储
                        sendAlarmKafka(SysDefine.FAULT_STREAM,faultTopic,vid, json);
                    }
                }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SysDefine.FAULT_STREAM, new Fields("TOPIC", DataKey.VEHICLE_ID, "VALUE"));
    }
    
    void sendAlarmKafka(String define,String topic,String vid, String message) {
        collector.emit(define, new Values(topic, vid, message));
    }
    
    boolean isNullOrEmpty(String string){
        if(null == string || "".equals(string))
            return true;
        return "".equals(string.trim());
    }
    
}
