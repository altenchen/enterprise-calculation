package storm.bolt.deal.cusmade;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.alibaba.fastjson.JSON;
import com.sun.jersey.core.util.Base64;

import storm.system.SysDefine;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;

@SuppressWarnings("all")
public class UserActionBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1700001L;
	private OutputCollector collector;
	private static String actionTopic;
	private long lastExeTime;
    private long flushtime;
    
    public static ScheduledExecutorService service;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        actionTopic = stormConf.get("kafka.topic.action").toString();
        lastExeTime=System.currentTimeMillis();
    }
    @Override
    public void execute(Tuple tuple) {
    	long now = System.currentTimeMillis();
        if (now - lastExeTime >= flushtime){
        	lastExeTime=now;
        }
    	if(tuple.getSourceStreamId().equals(SysDefine.YAACTION_GROUP)){
    		String vid = tuple.getString(0);
            Map<String, String> data = (TreeMap<String, String>) tuple.getValue(1);
            if (null == data.get("VID")) 
				data.put("VID", vid);

            List<Map<String, String>> actions = handle(data);
            if (null != actions) {
				for (Map<String, String> map : actions) {
					//action
		            /*String jsonString=JSON.toJSONString(map);//json 格式
		            jsonString=jsonString.replace("\"", "")
		            		.replace("{", "")
		            		.replace("}", "");*/
		            if(null != map && map.size()>0){
		            	StringBuilder sb = new StringBuilder();
		            	int cnt=0;
		            	int size=map.size();
		            	for (Map.Entry<String, String> entry : map.entrySet()) {
		            		sb.append(entry.getKey())
		            		.append(":")
		            		.append(entry.getValue());
		            		cnt++;
		            		if(cnt<size)
		            			sb.append(",");
						}
		            	//kafka存储
		            	sendAlarmKafka(SysDefine.YAACTION_NOTICE,actionTopic,vid, sb.toString());
		            }		
				}
			}
            
    	}
    	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream(SysDefine.YAACTION_NOTICE, new Fields("TOPIC", SysDefine.VID, "VALUE"));
    }
    
    void sendAlarmKafka(String define,String topic,String vid, String message) {
        collector.emit(define, new Values(topic, vid, message));
    }
    
    private List<Map<String, String>> handle(Map<String, String> data){

    	if (!isNullOrEmpty(data)) {
    		String vid = data.get("VID");
    		String actions = data.get("4001");
    		return analysisToListMap(vid,actions);
    	}
    	
    	return null;
    }
    
    private List<Map<String,String>> analysisToListMap(String vid,String base64s){
    	if(isNullOrEmpty(base64s))
    		return null;
    	List<Map<String,String>> list = null;
    	try {
			String[] arr = base64s.split("\\|");
			if (arr.length<1) 
				return list;
			list = new LinkedList<Map<String,String>>();
			for(int i =0;i<arr.length;i++){
				
				if (null !=arr[i]) {
					Map<String,String> map = analysisToMap(arr[i]);
					if (null != map) {
						map.put("VID", vid);
						list.add(map);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    	return list;
    }
    
    private Map<String,String> analysisToMap(String base64){
    	if(isNullOrEmpty(base64))
    		return null;
    	Map<String,String> map =null;
    	try {
			String string = new String(Base64.decode(base64),"UTF-8");
			String [] strings = string.split(",");
			
			if (strings.length == 3) {
				map = new TreeMap<String,String>();
				for(int i=0;i<3;i++){
					if (1 == i) 
						continue;

					String [] kv = strings[i].split(":");
					if(kv.length==2){
						if("1".equals(kv[0]))
							map.put("TYPE", new String(kv[1]));
						if("3".equals(kv[0])){
							String [] spcs = kv[1].split("_");
							if(6 == spcs.length){
								map.put("STTIME", new String(spcs[0]));
								map.put("EDTIME", new String(spcs[1]));
								map.put("STLONGIT", new String(spcs[2]));
								map.put("STLATITU", new String(spcs[3]));
								map.put("EDLONGIT", new String(spcs[4]));
								map.put("EDLATITU", new String(spcs[5]));
								
							}
						}
					}
				}
				
				
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    	
    	return map;
    }
    
    boolean isNullOrEmpty(Map map){
		if(map == null || map.size()==0)
			return true;
		return false;
	}
    boolean isNullOrEmpty(String string){
		if(null == string || "".equals(string))
			return true;
		return "".equals(string.trim());
	}
    
}
