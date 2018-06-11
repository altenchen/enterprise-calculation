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
    private long idleTimeoutMillsecond = 86400000;
    /**
     * 最后进行离线检查的时间, 用于离线判断
     */
    private long lastOfflineCheckTimeMillisecond;
    /**
     * 离线检查, 多长时间检查一下是否离线, 默认2分钟
     */
    private long offlineCheckSpanMillisecond = 120000;
    /**
     * 离线判定, 多长时间算是离线, 默认10分钟
     */
    private static long offlineTimeMillisecond = 600000;
    private CarRuleHandler carRuleHandler;
    private OnOffInfoNotice carOnOffhandler;
    private FaultCodeHandler faultCodeHandler;
    public static ScheduledExecutorService service;
    private static int ispreCp=0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        noticeTopic = stormConf.get("kafka.topic.notice").toString();

        long now = System.currentTimeMillis();
        lastOfflineCheckTimeMillisecond = now;

        try {
        	ParamsRedisUtil.rebulid();
            // 从Redis读取超时时间
			Object outbyconf = ParamsRedisUtil.PARAMS.get(ParamsRedisUtil.GT_INIDLE_TIME_OUT_SECOND);
			if (!ObjectUtils.isNullOrEmpty(outbyconf)) {
				idleTimeoutMillsecond =1000*(int)outbyconf;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

        // 多长时间算是离线
        String offLineSecond = ObjectUtils.getNullOrString(stormConf, StormConfigKey.REDIS_OFFLINE_SECOND);
        if (!ObjectUtils.isNullOrEmpty(offLineSecond)) {
			offlineTimeMillisecond = Long.parseLong(NumberUtils.stringNumber(offLineSecond))*1000;
		}

        // 多长时间检查一下是否离线
        String offLineCheckSpanSecond = ObjectUtils.getNullOrString(
            stormConf,
            StormConfigKey.REDIS_OFFLINE_CHECK_SPAN_SECOND
        );
        if (!ObjectUtils.isNullOrEmpty(offLineCheckSpanSecond)) {
        	offlineCheckSpanMillisecond = Long.parseLong(NumberUtils.stringNumber(offLineCheckSpanSecond))*1000;
        }

        carRuleHandler = new CarRuleHandler();
        try {
        	SysRealDataCache.init();
        	faultCodeHandler = new FaultCodeHandler();
            carOnOffhandler = new CarOnOffHandler();

            // region 如果从配置读到ispreCp为2, 则进行一次全量数据扫描, 并将告警数据发送到kafka
        	if (stormConf.containsKey(StormConfigKey.REDIS_CLUSTER_DATA_SYN)) {
        		Object precp = stormConf.get(StormConfigKey.REDIS_CLUSTER_DATA_SYN);
        		if (null != precp && !"".equals(precp.toString().trim())) {
        			ispreCp = Integer.valueOf(NumberUtils.stringNumber(precp.toString()));
        		}
        	}
    		if (2 == ispreCp){
    			carOnOffhandler.onOffCheck("TIMEOUT",0,now, offlineTimeMillisecond);
    			List<Map<String, Object>> msgs = carOnOffhandler.fulldoseNotice("TIMEOUT", ScanRange.AllData,now, idleTimeoutMillsecond);
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
    		
    		// region 每5分钟执行一次活跃数据扫描，将闲置车辆告警发到kafka中。
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
							CarRuleHandler.rebulid();
                            //从配置文件中读出超时时间
							Object outbyconf = ParamsRedisUtil.PARAMS.get(ParamsRedisUtil.GT_INIDLE_TIME_OUT_SECOND);
							if (!ObjectUtils.isNullOrEmpty(outbyconf)) {
								idleTimeoutMillsecond =1000*(int)outbyconf;
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						
						List<Map<String, Object>> msgs = carOnOffhandler.fulldoseNotice("TIMEOUT",ScanRange.AliveData,System.currentTimeMillis(), idleTimeoutMillsecond);
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
        	// 每5分钟执行一次
        	Executors
                .newScheduledThreadPool(1)
                .scheduleAtFixedRate(
                    new TimeOutClass(),
                    0,
                    300,
                    TimeUnit.SECONDS);
    		// endregion
        	
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }

    @Override
    public void execute(Tuple tuple) {
    	long now = System.currentTimeMillis();

        // region 离线判断: 如果时间差大于离线检查时间，则进行离线检查, 如果车辆离线，则发送此车辆的所有故障码结束通知
        if (now - lastOfflineCheckTimeMillisecond >= offlineCheckSpanMillisecond) {

        	lastOfflineCheckTimeMillisecond = now;
        	List<Map<String, Object>> msgs = faultCodeHandler.generateNotice(now);

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
        	msgs = carRuleHandler.offlineMethod(now);
        	if (null != msgs && msgs.size()>0) {
        		for (Map<String, Object> map : msgs) {
        			if (null != map && map.size() > 0) {
        				Object vid = map.get("vid");
        				String json=JSON.toJSONString(map);
        				sendToKafka(SysDefine.CUS_NOTICE,noticeTopic,vid, json);
        			}
        		}
        	}

        	carOnOffhandler.onOffCheck("TIMEOUT",1, now, offlineTimeMillisecond);
        }
        // endregion

    	if(CUS_NOTICE_GROUP.streamId.equals(tuple.getSourceStreamId())){
    		String vid = tuple.getString(0);
            Map<String, String> data = (TreeMap<String, String>) tuple.getValue(1);

            if (null == data.get(DataKey.VEHICLE_ID)) {
                data.put(DataKey.VEHICLE_ID, vid);
            }
            
            try {
				SysRealDataCache.updateCache(data, now);
			} catch (Exception e) {
				e.printStackTrace();
			}
            //返回车辆通知
            //先检查规则是否启用，启用了，则把dat放到相应的处理方法中。将返回结果放到list中，返回。
            List<Map<String, Object>> msgs = carRuleHandler.generateNotices(data);
            for(Map<String, Object> map: msgs) {
                if (null != map && map.size() > 0) {
                    String json=JSON.toJSONString(map);
                    sendToKafka(SysDefine.CUS_NOTICE, noticeTopic, vid, json);
                }
            }
        	
        	List<Map<String, Object>> faultCodeMessages = faultCodeHandler.generateNotice(data);
        	if (null != faultCodeMessages && faultCodeMessages.size()>0) {
				for (Map<String, Object> map : faultCodeMessages) {
					if (null != map && map.size() > 0) {
						String json=JSON.toJSONString(map);
						sendToKafka(SysDefine.CUS_NOTICE,noticeTopic,vid, json);
					}
				}
			}
        	
        	Map<String, Object> map = carOnOffhandler.generateNotices(data, now, offlineTimeMillisecond);
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
}
