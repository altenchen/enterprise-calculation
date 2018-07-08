package storm.bolt.deal.norm;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import storm.handler.cal.EsRealCalHandler;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.GsonUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("all")
public class SynEsculBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1700001L;
    private static final ConfigUtils configUtils = ConfigUtils.getInstance();
    private static final GsonUtils gson = GsonUtils.getInstance();
    private OutputCollector collector;
    private static String statusEsTopic;
    private long lastExeTime;
    private long offlinechecktime;
    private EsRealCalHandler handler;
    public static ScheduledExecutorService service;
    private static int ispreCp=0;
    private long rebootTime;
    public static long againNoproTime = 1800000;//处理积压数据，至少给予 半小时，1800秒时间
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        statusEsTopic = stormConf.get("kafka.topic.es.status").toString();
        long now = System.currentTimeMillis();
        lastExeTime = now;
        rebootTime = now;
        String nocheckObj = configUtils.sysDefine.getProperty("sys.reboot.nocheck");
        if (!StringUtils.isEmpty(nocheckObj)) {
            againNoproTime=Long.parseLong(nocheckObj)*1000;
        }
        offlinechecktime=Long.parseLong(stormConf.get("offline.check.time").toString());
        try {
            if (stormConf.containsKey("redis.cluster.data.syn")) {
                Object precp = stormConf.get("redis.cluster.data.syn");
                if (null != precp && !"".equals(precp.toString().trim())) {
                    String str = precp.toString();
                    ispreCp = Integer.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(str) ? str : "0");
                }
            }
            handler = new EsRealCalHandler();
            if (5 == ispreCp || 2 == ispreCp){

                List<Map<String, Object>> monitormsgs = handler.redisCarinfoSendMsgs();
                if (null != monitormsgs && monitormsgs.size()>0) {
                    System.out.println("---------------syn car is monitor or no--------total size:"+monitormsgs.size());
                    for (Map<String, Object> map : monitormsgs) {
                        if (null != map && map.size() > 0) {
                            Object vid = map.get(SysDefine.UUID);
                            String json=gson.toJson(map);
                            sendToKafka(SysDefine.SYNES_NOTICE,statusEsTopic,vid, json);
                        }
                    }
                }
            }
            if (2 == ispreCp){
                List<Map<String, Object>> msgs = handler.redisClusterSendMsgs();
                if (null != msgs && msgs.size()>0) {
                    System.out.println("---------------syn redis cluster data--------");
                    for (Map<String, Object> map : msgs) {
                        if (null != map && map.size() > 0) {
                            Object vid = map.get(SysDefine.UUID);
                            String json=gson.toJson(map);
                            sendToKafka(SysDefine.SYNES_NOTICE,statusEsTopic,vid, json);
                        }
                    }
                }

            }
            ispreCp=1;

            class AliveCarOff implements Runnable{

                @Override
                public void run() {
                    try {

                        List<Map<String, Object>> msgs = handler.checkAliveCarOffline();
                        if (null != msgs && msgs.size()>0) {
                            for (Map<String, Object> map : msgs) {
                                if (null != map && map.size() > 0) {
                                    Object vid = map.get(SysDefine.UUID);
                                    String json=gson.toJson(map);
                                    sendToKafka(SysDefine.SYNES_NOTICE,statusEsTopic,vid, json);
                                }
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new AliveCarOff(), 0, offlinechecktime, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void execute(Tuple tuple) {
        long now = System.currentTimeMillis();
        
        if(SysDefine.SYNES_GROUP.equals(tuple.getSourceStreamId())){
            String vid = tuple.getString(0);
            Map<String, String> data = (TreeMap<String, String>) tuple.getValue(1);
            if (null == data.get(DataKey.VEHICLE_ID)) {
                data.put(DataKey.VEHICLE_ID, vid);
            }

            Map<String, Object> esMap = handler.getSendEsMsgAndSetAliveLast(data,now);
            if (null != esMap && esMap.size()>0) {

                String json =gson.toJson(esMap);
                sendToKafka(SysDefine.SYNES_NOTICE,statusEsTopic,vid, json);
            }
        } else if(SysDefine.REG_STREAM_ID.equals(tuple.getSourceStreamId())){
            if (now - rebootTime <againNoproTime) {
                return;
            }
            String regMsg = tuple.getString(0);
            if (null != regMsg && regMsg.length() > 26 && regMsg.indexOf(SysDefine.COMMA) > 0 && regMsg.indexOf(SysDefine.COLON) > 0) {
                String [] params = regMsg.split(SysDefine.COMMA);
                Map<String, String> regMsgMap = new TreeMap<String, String>();
                for (String param : params) {
                    if (null != param) {
                        String [] items = param.split(SysDefine.COLON);
                        if (2 == items.length) {
                            regMsgMap.put(new String(items[0]), new String(items[1]));
                        } else {
                            regMsgMap.put(new String(items[0]),"");
                        }
                    }
                }
                if (regMsgMap.size() > 2) {
                     Map<String, Object> esMap = handler.getRegCarMsg(regMsgMap);
                     if (null != esMap && esMap.size()>0) {
                         Object vid = esMap.get(SysDefine.UUID);

                         String json =gson.toJson(esMap);
                         sendToKafka(SysDefine.SYNES_NOTICE,statusEsTopic,vid, json);
                     }
                }
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SysDefine.SYNES_NOTICE, new Fields("TOPIC", DataKey.VEHICLE_ID, "VALUE"));
    }
    
    void sendToKafka(String define,String topic,Object vid, String message) {
        collector.emit(define, new Values(topic, vid, message));
    }
    
    boolean isNullOrEmpty(Map map){
        if(map == null || map.size()==0) {
            return true;
        }
        return false;
    }
    boolean isNullOrEmpty(String string){
        if(null == string || "".equals(string)) {
            return true;
        }
        return "".equals(string.trim());
    }
    
}
