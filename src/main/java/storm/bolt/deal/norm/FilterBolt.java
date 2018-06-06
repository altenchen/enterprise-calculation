package storm.bolt.deal.norm;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.protocol.*;
import storm.system.DataKey;
import storm.util.*;
import storm.system.ProtocolItem;
import storm.system.SysDefine;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 1. 将报文转换成字典, 最后通过tuple发出
 * 2. 解析了告警并写入字典中
 * 3. 计算最大里程数和最小里程数, 并将当前里程写入字典
 * 4. 计算充电状态并写入字典
 *
 * Question: 最小里程数是不稳定的, 算出的里程数是指什么? 目前Storm内部没有使用, 前端是否还有使用?
 */
public class FilterBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1700001L;
	private OutputCollector collector;
//    public static long redisUpdateTime = 0L;
    
//    public static ScheduledExecutorService service;

    // 每辆车的最大里程数
    private Map<String, Long> maxMileMap;
    // 每辆车的最小里程数
    private Map<String, Long> minMileMap;

//    private Map<String, Integer> lastMile;
//    private Map<String, Integer> lastgpsDatMile;//(vid,当前里程值)最后一帧的里程
//    private Map<String, double[]> lastgps;//(vid,经纬度坐标x,y)最后一帧的gps坐标
//    private Map<String, String> lastgpsRegion;//(vid,所在区域id)最后一帧的区域

    // 每辆车的连续充电报文计次
    private Map<String, Integer> chargeMap;

    // 重启时间
    private long rebootTime;
    // 启动Storm计算时, kafka会从topic开始处消费, 所以需要一定的时间来扔掉这些数据.
    public static long againNoproTime = 1800 * 1000;//处理积压数据，至少给予 半小时，1800秒时间
//    AreaFenceHandler areaHandler;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        maxMileMap = new HashMap<String, Long>();
        minMileMap = new HashMap<String, Long>();
//        lastMile = new HashMap<String, Integer>();
//        lastgpsDatMile = new HashMap<String, Integer>();
//        lastgps = new HashMap<String, double[]>();
//        lastgpsRegion = new HashMap<String, String>();
        chargeMap = new HashMap<String, Integer>();

        rebootTime = System.currentTimeMillis();
        String nocheckObj = ConfigUtils.sysDefine.getProperty("sys.reboot.nocheck");
        if (! ObjectUtils.isNullOrEmpty(nocheckObj)) {
        	againNoproTime=Long.parseLong(nocheckObj)*1000;
		}
//        areaHandler = new AreaFenceHandler();
    }

    @Override
    public void execute(Tuple tuple) {

        // region Blot启动后againNoproTime的时长内, 忽略任何收到的数据
        // 启动Storm计算时, kafka会从topic开始处消费, 所以需要一定的时间来扔掉这些数据.
    	long now = System.currentTimeMillis();
    	if (now - rebootTime < againNoproTime) {
			return;
		}
		// endregion

        // tuple 结构是 (vid, message)
        if (tuple.size() == 2) {//实时数据  消息前缀 序列号 VIN码 命令标识 参数集 SUBMIT 1 LVBV4J0B2AJ063987 SUBMIT_LOGIN {1001:20150623120000,1002:京A12345}
            String[] parm = null;
            String[] message = null;
            String[] tempKV = null;

            message = StringUtils.split(tuple.getString(1), SysDefine.SPACES);
            if (message.length != 5 // 非业务包
                ||!CommandType.SUBMIT.equals(message[ProtocolSeparator.PREFIX])) { // 非主动发送
                return;
            }

            // 命令标识
            String type = message[ProtocolSeparator.COMMAND_TYPE];
            if (CommandType.SUBMIT_PACKET.equals(type)
            		|| SysDefine.RENTALSTATION.equals(type)
            		|| SysDefine.CHARGESTATION.equals(type)) {
                return;
            }


            // region 将参数拆分, 存储在stateKV中, 并附加VIN和命令类型到stateKV

            String content = message[ProtocolSeparator.CONTENT]; // 指令参数
            // 丢掉首尾大括号
            String usecontent = content.substring(1, content.length() - 1);
            // 拆分逗号分隔的参数组
            parm = StringUtils.split(usecontent, SysDefine.COMMA);

            Map<String, String> stateKV = new TreeMap<String, String>(); // 状态键值
            for (int i = 0; i < parm.length; i++) {
                tempKV = StringUtils.split(parm[i], SysDefine.COLON, 2);
                if (tempKV.length == 2) {
                    stateKV.put(new String(tempKV[0]), new String(tempKV[1]));
                }else if (tempKV.length == 1) {
                    stateKV.put(new String(tempKV[0]), "");
                }
            }
            tempKV=null;
            stateKV.put(SysDefine.VIN, new String(message[ProtocolSeparator.VIN]));
            stateKV.put(SysDefine.PREFIX, new String(message[ProtocolSeparator.PREFIX]));
            stateKV.put(SysDefine.MESSAGETYPE, new String(message[ProtocolSeparator.COMMAND_TYPE]));
            // endregion

            // region 计算在线状态(10002)和平台注册通知类型(TYPE)
            if (
                // 实时数据
                CommandType.SUBMIT_REALTIME.equals(type)
                // 终端注册
                || CommandType.SUBMIT_LOGIN.equals(type)
                // 状态信息上报
                || CommandType.SUBMIT_TERMSTATUS.equals(type)
                // 车辆运行状态
                || CommandType.SUBMIT_CARSTATUS.equals(type)) {

                // 设置在线状态(10002)为"1"
            	stateKV.put(SysDefine.ISONLINE, "1");

            	// 如果是终端注册报文
            	if (CommandType.SUBMIT_LOGIN.equals(type) ) {
                    // 如果stateKV包含登出流水号或者登出时间, 则设置在线状态(10002)为"0"
                    // 并且将`平台注册通知类型`设置为`车机离线`
                    // 否则将`平台注册通知类型`设置为`车机终端上线`
                    if(stateKV.containsKey(SUBMIT_LOGIN.LOGOUT_SEQ)
                        || stateKV.containsKey(SUBMIT_LOGIN.LOGOUT_TIME)) {

                        stateKV.put(SysDefine.ISONLINE, "0");
                        stateKV.put(ProtocolItem.REG_TYPE, "2");
                    } else {
                        stateKV.put(ProtocolItem.REG_TYPE, "1");
                    }
                    // TODO WARN: 上面的REG_TYPE值为TYPE, 这可能与SUBMIT_LINKSTATUS命令中的TYPE重叠了
                    // 注意进入此处说明不是SUBMIT_LINKSTATUS报文, 所以要看stateKV作用于是否仅限于当前报文
				}
            }
            // endregion

//            // region 如果是链接状态报文, 则记录UTC时间(10005)并计算是否在线(10002), 如果离线则同时将是否告警(10001)置空.
//            if (CommandType.SUBMIT_LINKSTATUS.equals(type)) { // 车辆链接状态 TYPE：1上线，2心跳，3离线
//                Map<String, String> linkmap = new TreeMap<String, String>();
//                if (SUBMIT_LINKSTATUS.isOnlineNotice(stateKV.get(SUBMIT_LINKSTATUS.LINK_TYPE))
//                		|| SUBMIT_LINKSTATUS.isHeartbeatNotice(stateKV.get(SUBMIT_LINKSTATUS.LINK_TYPE))) {
//                    linkmap.put(SysDefine.ISONLINE, "1");
//                } else if (SUBMIT_LINKSTATUS.isOfflineNotice(stateKV.get(SUBMIT_LINKSTATUS.LINK_TYPE))) {
//                    linkmap.put(SysDefine.ISONLINE, "0");
//                    linkmap.put(SysDefine.ISALARM, "0");
//                }
//                linkmap.put(SysDefine.ONLINEUTC, String.valueOf(now)); // 增加utc字段，插入系统时间
//            }
//            // endregion

            // region 如果是补发数据直接忽略
            if (CommandType.SUBMIT_HISTORY.equals(type)) {//补发历史原始数据存储
//            	sendMessages(SysDefine.SUPPLY_GROUP,null,vid,stateKV,true);
            	return;
            }
            // endregion
            
            // region 计算时间(TIME)加入stateKV
            if (CommandType.SUBMIT_REALTIME.equals(type)) {
                // 如果是实时数据, 则将TIME设置为数据采集时间
                stateKV.put(SysDefine.TIME, stateKV.get(DataKey._2000_COLLECT_TIME));
            } else if (CommandType.SUBMIT_LOGIN.equals(type)) {
                // 如果是注册报文, 则将TIME设置为登入时间或者登出时间或者注册时间
            	if (stateKV.containsKey(SUBMIT_LOGIN.LOGIN_TIME)) {
                    // 将TIME设置为登入时间
					stateKV.put(SysDefine.TIME, stateKV.get(SUBMIT_LOGIN.LOGIN_TIME));
				} else if (stateKV.containsKey(SUBMIT_LOGIN.LOGOUT_TIME)){
                    // 将TIME设置为登出时间
					stateKV.put(SysDefine.TIME, stateKV.get(SUBMIT_LOGIN.LOGOUT_TIME));
				} else {
                    // 将TIME设置为注册时间
					stateKV.put(SysDefine.TIME, stateKV.get(SUBMIT_LOGIN.REGIST_TIME));
				}
            } else if (CommandType.SUBMIT_TERMSTATUS.equals(type)) {
                // 如果是状态信息上报, 则将TIME设置为采集时间
                stateKV.put(SysDefine.TIME, stateKV.get("3101"));
            } else if (CommandType.SUBMIT_HISTORY.equals(type)) {
                stateKV.put(SysDefine.TIME, stateKV.get("2000"));
            } else if (CommandType.SUBMIT_CARSTATUS.equals(type)) {
                stateKV.put(SysDefine.TIME, stateKV.get("3201"));
            } else if (SysDefine.RENTCAR.equals(type)) { // 租赁数据
                stateKV.put(SysDefine.TIME, stateKV.get("4001"));
            } else if (SysDefine.CHARGE.equals(type)) { // 充电设施数据
                stateKV.put(SysDefine.TIME, stateKV.get("4101"));
            }
            // endregion

            stateKV.put(SysDefine.ONLINEUTC, now + ""); // 增加utc字段，插入系统时间
            try {
            	if (CommandType.SUBMIT_REALTIME.equals(type)
            			|| CommandType.SUBMIT_HISTORY.equals(type))
            		processValid(stateKV);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
            Map<String, String> stateNewKV = stateKV; // 状态键值
            if (CommandType.SUBMIT_LINKSTATUS.equals(type)
        			|| CommandType.SUBMIT_LOGIN.equals(type)
        			|| CommandType.SUBMIT_REALTIME.equals(type)
        			|| CommandType.SUBMIT_TERMSTATUS.equals(type)){
                // 克隆一份?
            	stateNewKV = new TreeMap<String, String>();
            	stateNewKV.putAll(stateKV);
            }

            String vid = stateKV.get(SysDefine.VID);
            try {
            	if (CommandType.SUBMIT_LINKSTATUS.equals(type)
            			|| CommandType.SUBMIT_LOGIN.equals(type)
            			|| CommandType.SUBMIT_TERMSTATUS.equals(type)
            			|| CommandType.SUBMIT_CARSTATUS.equals(type)) {
            	    // consumer: ES数据同步处理
            		sendMessages(SysDefine.SYNES_GROUP,null,vid,stateNewKV,true);
            	}
            	if (CommandType.SUBMIT_REALTIME.equals(type)){
            	    // consumer: 电子围栏告警处理
            		sendMessages(SysDefine.FENCE_GROUP,null,vid,stateNewKV,true);
            		// consumer: 雅安用户行为处理
//            		sendMessages(SysDefine.YAACTION_GROUP,null,vid,stateKV,true);
            	}
            	if (CommandType.SUBMIT_REALTIME.equals(type)
            			|| CommandType.SUBMIT_LINKSTATUS.equals(type)
            			|| CommandType.SUBMIT_LOGIN.equals(type)
            			|| CommandType.SUBMIT_TERMSTATUS.equals(type)
            			|| CommandType.SUBMIT_CARSTATUS.equals(type)){
            	    // consumer: SOC与超时处理
            		sendMessages(SysDefine.CUS_NOTICE_GROUP,null,vid,stateNewKV,true);
            	}
            	if (CommandType.SUBMIT_REALTIME.equals(type)
            			|| CommandType.SUBMIT_LINKSTATUS.equals(type)
            			|| CommandType.SUBMIT_LOGIN.equals(type)){
            	    // 预警处理
            		sendMessages(SysDefine.SPLIT_GROUP,null,vid,stateKV,true);
            	}
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    //synchronized
    private void sendMessages(String streamId, String topic, String vid, Object data, boolean isHistory) {
    	if(isHistory)
    		collector.emit(streamId, new Values(vid, data));
    	else
    	    // SysDefine.HISTORY send to KafkaBlot
    		collector.emit(streamId, new Values(topic, vid, data));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declareStream(SysDefine.SUPPLY_GROUP, new Fields(SysDefine.VID, "DATA"));
        declarer.declareStream(SysDefine.SPLIT_GROUP, new Fields(SysDefine.VID, "DATA"));
        declarer.declareStream(SysDefine.FENCE_GROUP, new Fields(SysDefine.VID, "DATA"));
        declarer.declareStream(SysDefine.YAACTION_GROUP, new Fields(SysDefine.VID, "DATA"));
        declarer.declareStream(SysDefine.SYNES_GROUP, new Fields(SysDefine.VID, "DATA"));
        declarer.declareStream(SysDefine.CUS_NOTICE_GROUP, new Fields(SysDefine.VID, "DATA"));
        declarer.declareStream(SysDefine.HISTORY, new Fields("TOPIC", SysDefine.VID, "VALUE"));
    }

//    public Map<String, Object> getComponentConfiguration() {
//        return null;
//    }

    @Nullable
    private Map<String, String> processValid(@NotNull Map<String, String> dataMap) {
        if(ObjectUtils.isNullOrEmpty(dataMap))
        	return null;
        
    	String vid = dataMap.get(DataKey.VEHICLE_ID);

    	// region 更新所有车的最大里程数和最小里程数
        try {
            // 判断处理实时里程数据
            if (dataMap.containsKey(SUBMIT_REALTIME.TOTAL_MILEAGE)
                && !"".equals(dataMap.get(SUBMIT_REALTIME.TOTAL_MILEAGE))) {

                long mileage = Long.parseLong(
                    NumberUtils.stringNumber(
                        dataMap.get(SUBMIT_REALTIME.TOTAL_MILEAGE)
                    )
                );
                Long maxCacheMileage = maxMileMap.get(vid);
                Long minCacheMileage = minMileMap.get(vid);
                maxCacheMileage = ObjectUtils.isNullOrEmpty(maxCacheMileage) ? 0L : maxCacheMileage;
                minCacheMileage = ObjectUtils.isNullOrEmpty(minCacheMileage) ? 0L : minCacheMileage;
                long maxMileage = maxCacheMileage;
                long minMileage = minCacheMileage;

                if (mileage > maxCacheMileage) {
                    // redisService.saveRealtimeMessageByDataId(vid,"maxmileage",mileage+"",jedis);
                	maxMileMap.put(vid, mileage); // 更新缓存最大里程
                    maxMileage = mileage;
                }
                if (minCacheMileage == 0 || mileage < minCacheMileage) {
                    // redisService.saveRealtimeMessageByDataId(vid,"minmileage",mileage+"",jedis);
                	minMileMap.put(vid, mileage);
                    minMileage = mileage;
                }
                dataMap.put(SysDefine.MILEAGE, maxMileage - minMileage + "");
            }
        } catch (Exception e1) {
        	e1.printStackTrace();
        	System.out.println("预处理里程" + e1);
        }
        // endregion

        // region 连续10次处于充电状态并且车速为0, 则记录充电状态为"1", 否则重置次数为0.
        try {
            // 充放电状态 1充电，2放电
            final String charging = "1";
            final String disCharging = "2";
            if (CommandType.SUBMIT_REALTIME.equals(dataMap.get(SysDefine.MESSAGETYPE))) {
                if (dataMap.containsKey(SUBMIT_REALTIME.CHARGE_STATUS)
                    && !ObjectUtils.isNullOrEmpty(dataMap.get(SUBMIT_REALTIME.CHARGE_STATUS))) {
                    String status = dataMap.get(SUBMIT_REALTIME.CHARGE_STATUS);
                    if (chargeMap.get(vid) == null) 
                    	chargeMap.put(vid, 0);
                    
                    int cacheNum = chargeMap.get(vid);
                    if (charging.equals(status) &&
                        // 车速为0
                        "0".equals(NumberUtils.stringNumber(dataMap.get(SUBMIT_REALTIME.SPEED)))) {
                    	chargeMap.put(vid, cacheNum + 1);
                        if (cacheNum >= 10) {
                            dataMap.put(SysDefine.ISCHARGE, "1");
                        }
                    } else {
                        dataMap.put(SysDefine.ISCHARGE, "0");
                        chargeMap.put(vid, 0);
                    }
                } else {
                    // 如果不包含充电状态, 则记录充电状态为"0"
                    dataMap.put(SysDefine.ISCHARGE, "0");
                    chargeMap.put(vid, 0);
                }
            }
        } catch (Exception e) {
            System.out.println("----判断是否充电异常！" + e);
        }
        // endregion

        // region [忽略] 向最后的数据中加入车辆当前所在行政区域id
        /*try {
			if (dataMap.containsKey(ProtocolItem.LONGITUDE)
					&& dataMap.containsKey(ProtocolItem.LATITUDE)) {
				
				String longit = dataMap.get(ProtocolItem.LONGITUDE);
				String latit = dataMap.get(ProtocolItem.LATITUDE);
				if (null != longit && null != latit) {
					longit = NumberUtils.stringNumber(longit);
					latit = NumberUtils.stringNumber(latit);
					if (!"0".equals(longit)
							&& !"0".equals(latit)) {
						double x = Double.parseDouble(longit);
						double y = Double.parseDouble(latit);
						//是否要重新计算，flase为需要重新计算。
						boolean isNotCal = false;
						//当前里程
						int mileagenow = Integer.parseInt(NumberUtils.stringNumber(dataMap.get("2202")));
						if (lastgpsRegion.containsKey(vid)) {
							String lastRegion = lastgpsRegion.get(vid);
							
							if (lastgpsDatMile.containsKey(vid)) {
								int lastMileage = lastgpsDatMile.get(vid);
								if (0!= mileagenow && Math.abs(mileagenow-lastMileage) < 50) {
									isNotCal = true;
								}
							}
							if (!isNotCal && lastgps.containsKey(vid)) {
								double [] gpss = lastgps.get(vid);
								double distance = GpsUtil.getDistance(x, y, gpss[0], gpss[1]);
								//如果上次和这次gps之间的距离小于5公里，则不计算新的所属区域，默认还在上一帧的区域中。
								if (distance < 5) {
									isNotCal = true;
								}
							}
							//如果不计算则把上一帧中的区域算作这次的。
							if (isNotCal) {
								dataMap.put(ProtocolItem.GPS_ADMIN_REGION, lastRegion);
							} 
						} 
						
						if (!isNotCal) {//需要重新计算
							//获得（x,y）的区域id集合
							List<String> areaIds = areaHandler.areaIds(x,y);
							if (null != areaIds) {
								String areas = null;
								
								if (areaIds.size() == 1) {
									areas = areaIds.get(0);
								} else {
									StringBuilder sb = new StringBuilder();
									for (String area : areaIds) {
										sb.append(area).append("|");
									}
									areas = sb.substring(0, sb.length()-1);
								}
								//areas为所有的区域id拼接字符串
								dataMap.put(ProtocolItem.GPS_ADMIN_REGION, areas);
								lastgpsRegion.put(vid, areas);
								lastgps.put(vid, new double[]{x,y});
								lastgpsDatMile.put(vid, mileagenow);
							} else {
								dataMap.put(ProtocolItem.GPS_ADMIN_REGION, "UNKNOWN");
							}
						}
	                	
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
        */
        // endregion

        // 动力蓄电池报警标志
		/*
		 * 1：温度差异报警；0：正常 1：电池极柱高温报警；0：正常 1：动力蓄电池包过压报警；0：正常 1：动力蓄电池包欠压报警；0：正常 1：SOC低报警；0：正常 1：单体蓄电池过压报警；0：正常 1：单体蓄电池欠压报警；0：正常 1：SOC太低报警；0：正常 1：SOC过高报警；0：正常 1：动力蓄电池包不匹配报警；0：正常 1：动力蓄电池一致性差报警；0：正常 1：绝缘故障；0：正常
		 */
        try {
            // region 北京地标: 动力蓄电池报警标志解析存储, 见表20
            if (dataMap.containsKey(SUBMIT_REALTIME.POWER_BATTERY_ALARM_FLAG_2801)
                && !"".equals(dataMap.get(SUBMIT_REALTIME.POWER_BATTERY_ALARM_FLAG_2801))) {
                // WORD通过二进制标识(预留部分填0)
                String binaryStr = TimeUtils.fillNBitBefore(
                    Long.toBinaryString(
                        Long.parseLong(
                            NumberUtils.stringNumber(
                                dataMap.get(SUBMIT_REALTIME.POWER_BATTERY_ALARM_FLAG_2801)
                            )
                        )
                    ),
                    12,
                    "0");
                // 绝缘故障
                dataMap.put("2912", new String(binaryStr.substring(0, 1)));
                // 动力蓄电池包不匹配报警
                dataMap.put("2911", new String(binaryStr.substring(1, 2)));
                // 动力蓄电池包不匹配报警
                dataMap.put("2910", new String(binaryStr.substring(2, 3)));
                // SOC过高报警
                dataMap.put("2909", new String(binaryStr.substring(3, 4)));
                // SOC太低报警
                dataMap.put("2908", new String(binaryStr.substring(4, 5)));
                // 单体蓄电池欠压报警
                dataMap.put("2907", new String(binaryStr.substring(5, 6)));
                // 单体蓄电池过压报警
                dataMap.put("2906", new String(binaryStr.substring(6, 7)));
                // SOC低报警
                dataMap.put("2905", new String(binaryStr.substring(7, 8)));
                // 动力蓄电池包欠压报警
                dataMap.put("2904", new String(binaryStr.substring(8, 9)));
                // 动力蓄电池包过压报警
                dataMap.put("2903", new String(binaryStr.substring(9, 10)));
                // 电池极柱高温报警
                dataMap.put("2902", new String(binaryStr.substring(10, 11)));
                // 温度差异报警
                dataMap.put("2901", new String(binaryStr.substring(11, 12)));
                binaryStr=null;
            }
            // endregion

            // region 国标: 通用报警标志值, 见表18
            else if(dataMap.containsKey(SUBMIT_REALTIME.ALARM_MARK) && !"".equals(dataMap.get(SUBMIT_REALTIME.ALARM_MARK))) {
                String binaryStr = TimeUtils.fillNBitBefore(
                    Long.toBinaryString(
                        Long.parseLong(
                            NumberUtils.stringNumber(
                                dataMap.get(SUBMIT_REALTIME.ALARM_MARK)
                            )
                        )
                    ),
                    32,
                    "0");
                dataMap.put("2919", new String(binaryStr.substring(13, 14)));//车载储能装置类型过充(第18位)
                dataMap.put("2918", new String(binaryStr.substring(14, 15)));//驱动电机温度报警
                dataMap.put("2917", new String(binaryStr.substring(15, 16)));//高压互锁状态报警
                dataMap.put("2916", new String(binaryStr.substring(16, 17)));//驱动电机控制器温度报警
                dataMap.put("2915", new String(binaryStr.substring(17, 18)));//DC-DC状态报警
                dataMap.put("2914", new String(binaryStr.substring(18, 19)));//制动系统报警
                dataMap.put("2913", new String(binaryStr.substring(19, 20)));//DC-DC温度报警
                dataMap.put("2912", new String(binaryStr.substring(20, 21)));//绝缘报警
                dataMap.put("2911", new String(binaryStr.substring(21, 22)));//动力蓄电池一致性差报警
                dataMap.put("2910", new String(binaryStr.substring(22, 23)));//可充电储能系统不匹配报警
                dataMap.put("2930", new String(binaryStr.substring(23, 24)));//SOC跳变报警
                dataMap.put("2909", new String(binaryStr.substring(24, 25)));//SOC过高报警
                dataMap.put("2907", new String(binaryStr.substring(25, 26)));//单体电池欠压报警
                dataMap.put("2906", new String(binaryStr.substring(26, 27)));//单体电池过压报警
                dataMap.put("2905", new String(binaryStr.substring(27, 28)));//SOC低报警
                dataMap.put("2904", new String(binaryStr.substring(28, 29)));//车载储能装置类型欠压报警
                dataMap.put("2903", new String(binaryStr.substring(29, 30)));//车载储能装置类型过压报警
                dataMap.put("2902", new String(binaryStr.substring(30, 31)));//电池高温报警
                dataMap.put("2901", new String(binaryStr.substring(31, 32)));//温度差异报警(第0位)
                
                generalAlarmToFaultCode(dataMap, binaryStr);
            }
            // endregion
        } catch (Exception e) {
            System.out.println("----动力蓄电池报警标志处理异常！" + e);
        }

        // region 北京地标: 车载终端状态解析存储
        // 车载终端状态3110
		/*
		 * 1：通电；0：断开 BIT 3102 1：电源正常；0：电源异常 BIT 3103 1：通信传输正常；0：通信传输异常 BIT 3104 其他异常，1：正常；0：异常 BIT 3105
		 */
        try {
            if (dataMap.containsKey(DataKey._3110_STATUS_FLAGS) && !"".equals(dataMap.get(DataKey._3110_STATUS_FLAGS))) {
                String binaryStr = TimeUtils.fillNBitBefore(
                    Long.toBinaryString(
                        Long.parseLong(
                            NumberUtils.stringNumber(
                                dataMap.get(DataKey._3110_STATUS_FLAGS)
                            )
                        )
                    ),
                    4,
                    "0");
                dataMap.put("3105", new String(binaryStr.substring(0, 1)));
                dataMap.put("3104", new String(binaryStr.substring(1, 2)));
                dataMap.put("3103", new String(binaryStr.substring(2, 3)));
                dataMap.put("3102", new String(binaryStr.substring(3, 4)));
                binaryStr=null;
            }
        } catch (Exception e) {
            System.out.println("----车载终端状态标志处理异常！" + e);
        }
        // endregion

        return dataMap;
    }

    // 解析并填充其他故障列表
    private void generalAlarmToFaultCode(@NotNull Map<String, String> dataMap, @NotNull String binaryStr){

        // region 解析告警值
    	String unit19 = new String(binaryStr.substring(12, 13));//第19位
        String unit20 = new String(binaryStr.substring(11, 12));//第20位
        String unit21 = new String(binaryStr.substring(10, 11));//第21位
        String unit22 = new String(binaryStr.substring(9, 10));//第22位
        String unit23 = new String(binaryStr.substring(8, 9));//第23位
        String unit24 = new String(binaryStr.substring(7, 8));//第24位
        String unit25 = new String(binaryStr.substring(6, 7));//第25位
        String unit26 = new String(binaryStr.substring(5, 6));//第26位
        String unit27 = new String(binaryStr.substring(4, 5));//第27位
        String unit28 = new String(binaryStr.substring(3, 4));//第28位
        String unit29 = new String(binaryStr.substring(2, 3));//第29位
        String unit30 = new String(binaryStr.substring(1, 2));//第30位
        String unit31 = new String(binaryStr.substring(0, 1));//第31位
        dataMap.put("380119", unit19);
        dataMap.put("380120", unit20);
        dataMap.put("380121", unit21);
        dataMap.put("380122", unit22);
        dataMap.put("380123", unit23);
        dataMap.put("380124", unit24);
        dataMap.put("380125", unit25);
        dataMap.put("380126", unit26);
        dataMap.put("380127", unit27);
        dataMap.put("380128", unit28);
        dataMap.put("380129", unit29);
        dataMap.put("380130", unit30);
        dataMap.put("380131", unit31);
        // endregion

        // region 拼装其他故障代码列表(2809)
        StringBuffer sb = new StringBuffer();
        // 如果值为1, 则追加key01, 否则追加key00, 多个项之间用|连接
        sb
            .append("1".equals(unit19) ? "38011901" : "38011900").append("|")
            .append("1".equals(unit20) ? "38012001" : "38012000").append("|")
            .append("1".equals(unit21) ? "38012101" : "38012100").append("|")
            .append("1".equals(unit22) ? "38012201" : "38012200").append("|")
            .append("1".equals(unit23) ? "38012301" : "38012300").append("|")
            .append("1".equals(unit24) ? "38012401" : "38012400").append("|")
            .append("1".equals(unit25) ? "38012501" : "38012500").append("|")
            .append("1".equals(unit26) ? "38012601" : "38012600").append("|")
            .append("1".equals(unit27) ? "38012701" : "38012700").append("|")
            .append("1".equals(unit28) ? "38012801" : "38012800").append("|")
            .append("1".equals(unit29) ? "38012901" : "38012900").append("|")
            .append("1".equals(unit30) ? "38013001" : "38013000").append("|")
            .append("1".equals(unit31) ? "38013101" : "38013100");
        
        String faultCode2809 = dataMap.get("2809");
        if (null != faultCode2809 && ! "".equals(faultCode2809.trim())) {
        	sb.append("|").append(faultCode2809);
        }
        dataMap.put("2809", sb.toString());
        // endregion
        
//        .append("1".equals(unit00)?"38010001":"38010000")
//        .append("1".equals(unit01)?"38010101":"38010100")
//        .append("1".equals(unit02)?"38010201":"38010200")
//        .append("1".equals(unit03)?"38010301":"38010300")
//        .append("1".equals(unit04)?"38010401":"38010400")
//        .append("1".equals(unit05)?"38010501":"38010500")
//        .append("1".equals(unit06)?"38010601":"38010600");
    }
}
