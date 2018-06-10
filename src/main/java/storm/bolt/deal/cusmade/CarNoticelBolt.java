package storm.bolt.deal.cusmade;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.alibaba.fastjson.JSON;

import storm.cache.SysRealDataCache;
import storm.handler.FaultCodeHandler;
import storm.handler.cusmade.*;
import storm.stream.CUS_NOTICE_GROUP;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.NumberUtils;
import storm.util.ObjectUtils;
import storm.util.ParamsRedisUtil;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class CarNoticelBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1700001L;

	private OutputCollector collector;

    /**
     * 输出到Kafka的主题
     */
	private String noticeTopic;

    /**
     * 闲置车辆判定, 达到闲置状态时长, 默认1天
     */
    private long timeoutTime = 86400000;
    /**
     * 最后进行离线检查的时间, 用于离线判断
     */
    private long lastOfflineCheckTime;
    /**
     * 离线检查间隔2分钟
     */
    private long offlineCheckSpan = 120000;
    private static long offlinetime = 600000;//600秒
    private InfoNotice carRulehandler;
    private OnOffInfoNotice carOnOffhandler;
    private FaultCodeHandler codeHandler;
    public static ScheduledExecutorService service;
    private static int ispreCp=0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        noticeTopic = stormConf.get("kafka.topic.notice").toString();

        long now = System.currentTimeMillis();
        lastOfflineCheckTime = now;

        try {
        	ParamsRedisUtil.rebulid();
            // 从Redis读取超时时间
			Object outbyconf = ParamsRedisUtil.PARAMS.get(ParamsRedisUtil.GT_INIDLE_TIME_OUT_SECOND);
			if (!ObjectUtils.isNullOrEmpty(outbyconf)) {
				timeoutTime =1000*(int)outbyconf;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

        // 多长时间算是离线
        Object off = stormConf.get(StormConfigKey.REDIS_OFFLINE_SECOND);
        if (!ObjectUtils.isNullOrEmpty(off)) {
			offlinetime = Long.parseLong(NumberUtils.stringNumber(off.toString()))*1000;
		}

        // 多长时间检查一下是否离线
        Object offCheck = stormConf.get(StormConfigKey.REDIS_OFFLINE_CHECK_SPAN_SECOND);
        if (!ObjectUtils.isNullOrEmpty(offCheck)) {
        	offlineCheckSpan = Long.parseLong(NumberUtils.stringNumber(offCheck.toString()))*1000;
        }

        carRulehandler = new CarRulehandler();
        try {
        	SysRealDataCache.init();
        	codeHandler = new FaultCodeHandler();
            carOnOffhandler = new CarOnOffHandler();

            // region 如果从配置读到ispreCp为2, 则进行一次全量数据扫描
        	if (stormConf.containsKey(StormConfigKey.REDIS_CLUSTER_DATA_SYN)) {
        		Object precp = stormConf.get(StormConfigKey.REDIS_CLUSTER_DATA_SYN);
        		if (null != precp && !"".equals(precp.toString().trim())) {
        			ispreCp = Integer.valueOf(NumberUtils.stringNumber(precp.toString()));
        		}
        	}
    		if (2 == ispreCp){
    			carOnOffhandler.onoffCheck("TIMEOUT",0,now,offlinetime);
    			List<Map<String, Object>> msgs = carOnOffhandler.fulldoseNotice("TIMEOUT", ScanRange.AllData,now, timeoutTime);
    			if (null != msgs && msgs.size()>0) {
    				System.out.println("---------------syn redis cluster data--------");
    				for (Map<String, Object> map : msgs) {
    					if (null != map && map.size() > 0) {
    						Object vid = map.get("vid");
    						String json=JSON.toJSONString(map);
    						sendToKafka(SysDefine.CUS_NOTICE,noticeTopic,vid, json);
    					}
    				}
    			}
    			
    		}
    		ispreCp=1;
        	// endregion
    		
    		//定义一个定时任务，每隔一段时间将闲置车辆发到kafka中。
    		class TimeOutClass implements Runnable{

				@Override
				public void run() {
					try {
						
						try {
//							ParamsRedis.rebulid();
							/**
							 * 重新初始化 配置参数，里程跳变数字、未定位的 判断次数等
							 * 由于此方法内部已经调用了 ParamsRedis.rebulid()
							 * 因此可以省略 ParamsRedis 重新初始化方法
							 */
							CarRulehandler.rebulid();
							Object outbyconf = ParamsRedisUtil.PARAMS.get("gt.inidle.timeOut.time");//从配置文件中读出超时时间
							if (!ObjectUtils.isNullOrEmpty(outbyconf)) {
								timeoutTime =1000*(int)outbyconf;
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						
						List<Map<String, Object>> msgs = carOnOffhandler.fulldoseNotice("TIMEOUT",ScanRange.AliveData,System.currentTimeMillis(), timeoutTime);
			        	if (null != msgs && msgs.size()>0) {
							for (Map<String, Object> map : msgs) {
								if (null != map && map.size() > 0) {
									Object vid = map.get("vid");
									String json=JSON.toJSONString(map);
									sendToKafka(SysDefine.CUS_NOTICE,noticeTopic,vid, json);
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
        		
        	}
        	Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new TimeOutClass(), 0, 300, TimeUnit.SECONDS);
        	
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
    @Override
    public void execute(Tuple tuple) {
    	long now = System.currentTimeMillis();
        //如果时间差大于离线检查时间，则进行离线检查,如果车辆离线，则发送此车辆的所有故障码结束通知
        if (now - lastOfflineCheckTime >= offlineCheckSpan){
        	lastOfflineCheckTime =now;
        	List<Map<String, Object>> msgs = codeHandler.handle(now);
        	
        	if (null != msgs && msgs.size()>0) {
        		for (Map<String, Object> map : msgs) {
        			if (null != map && map.size() > 0) {
        				Object vid = map.get("vid");
        				String json=JSON.toJSONString(map);
        				sendToKafka(SysDefine.CUS_NOTICE,noticeTopic,vid, json);
        			}
        		}
        	}
        	//检查所有车辆是否离线，离线则发送离线通知。
        	msgs = carRulehandler.offlineMethod(now);
        	if (null != msgs && msgs.size()>0) {
        		for (Map<String, Object> map : msgs) {
        			if (null != map && map.size() > 0) {
        				Object vid = map.get("vid");
        				String json=JSON.toJSONString(map);
        				sendToKafka(SysDefine.CUS_NOTICE,noticeTopic,vid, json);
        			}
        		}
        	}

        	carOnOffhandler.onoffCheck("TIMEOUT",1,now,offlinetime);
        }
    	if(CUS_NOTICE_GROUP.streamId.equals(tuple.getSourceStreamId())){
    		String vid = tuple.getString(0);
            Map<String, String> data = (TreeMap<String, String>) tuple.getValue(1);
            if (null == data.get(DataKey.VEHICLE_ID))
				data.put(DataKey.VEHICLE_ID, vid);
            
            try {
				SysRealDataCache.addCaChe(data,now);
			} catch (Exception e) {
				e.printStackTrace();
			}
            //返回车辆通知
            //先检查规则是否启用，启用了，则把dat放到相应的处理方法中。将返回结果放到list中，返回。
            List<Map<String, Object>> msgs = carRulehandler.generateNotices(data);
            for(Map<String, Object> map: msgs) {
                if (null != map && map.size() > 0) {
                    String json=JSON.toJSONString(map);
                    sendToKafka(SysDefine.CUS_NOTICE, noticeTopic, vid, json);
                }
            }
        	
        	List<Map<String, Object>> faultcodemsgs = codeHandler.handle(data);
        	if (null != faultcodemsgs && faultcodemsgs.size()>0) {
				for (Map<String, Object> map : faultcodemsgs) {
					if (null != map && map.size() > 0) {
						String json=JSON.toJSONString(map);
						sendToKafka(SysDefine.CUS_NOTICE,noticeTopic,vid, json);
					}
				}
			}
        	
        	Map<String, Object> map = carOnOffhandler.genotice(data, now, offlinetime);
        	if (null != map && map.size() > 0) {
				String json=JSON.toJSONString(map);
				sendToKafka(SysDefine.CUS_NOTICE,noticeTopic,vid, json);
			}
    	} 
    	
    	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream(SysDefine.CUS_NOTICE, new Fields("TOPIC", DataKey.VEHICLE_ID, "VALUE"));
    }
    
    void sendToKafka(String define,String topic,Object vid, String message) {
        collector.emit(define, new Values(topic, vid, message));
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
