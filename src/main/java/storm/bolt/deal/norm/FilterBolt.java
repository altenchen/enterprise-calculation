package storm.bolt.deal.norm;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import storm.util.*;
import storm.handler.area.AreaFenceHandler;
import storm.system.ProtocolItem;
import storm.system.SysDefine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;

public class FilterBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1700001L;
	private OutputCollector collector;
    public static long redisUpdateTime = 0L;
    
    public static ScheduledExecutorService service;
    private Map<String, Long> maxMileMap;
    private Map<String, Long> minMileMap;
    private Map<String, Integer> lastMile;
    private Map<String, Integer> lastgpsDatMile;//(vid,当前里程值)最后一帧的里程
    private Map<String, double[]> lastgps;//(vid,经纬度坐标x,y)最后一帧的gps坐标
    private Map<String, String> lastgpsRegion;//(vid,所在区域id)最后一帧的区域
    private Map<String, Integer> chargeMap;
    private long rebootTime;
    public static long againNoproTime = 1800000;//处理积压数据，至少给予 半小时，1800秒时间
    AreaFenceHandler areaHandler;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        maxMileMap = new HashMap<String, Long>();
        minMileMap = new HashMap<String, Long>();
        lastMile = new HashMap<String, Integer>();
        lastgpsDatMile = new HashMap<String, Integer>();
        lastgps = new HashMap<String, double[]>();
        lastgpsRegion = new HashMap<String, String>();
        chargeMap = new HashMap<String, Integer>();
        System.currentTimeMillis();
        long now = System.currentTimeMillis();
        rebootTime = now;
        String nocheckObj = ConfigUtils.sysDefine.getProperty("sys.reboot.nocheck");
        if (! ObjectUtils.isNullOrEmpty(nocheckObj)) {
        	againNoproTime=Long.parseLong(nocheckObj)*1000;
		}
        areaHandler = new AreaFenceHandler();
    }
    @Override
    public void execute(Tuple tuple) {
    	long now = System.currentTimeMillis();
    	if (now - rebootTime <againNoproTime) {
			return;
		}
        if (tuple.size() == 2) {//实时数据  消息前缀 序列号 VIN码 命令标识 参数集 SUBMIT 1 LVBV4J0B2AJ063987 LOGIN {1001:20150623120000,1002:京A12345}
            String[] parm = null;
            String[] message = null;
            String[] tempKV = null;

            message = StringUtils.split(tuple.getString(1), SysDefine.SPACES);
            if (message.length != 5 // 非业务包
            		||!SysDefine.SUBMIT.equals(message[0])) { // 非主动发送
                return;
            }

            String type = message[3];
            if (SysDefine.PACKET.equals(type) 
            		|| SysDefine.RENTALSTATION.equals(type) 
            		|| SysDefine.CHARGESTATION.equals(type)) {
                return;
            }

            String content = message[4]; // 指令参数
            String usecontent = content.substring(1, content.length() - 1);
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
            stateKV.put(SysDefine.VIN, new String(message[2]));
            stateKV.put(SysDefine.MESSAGETYPE, new String(message[3]));
 
            if (SysDefine.REALTIME.equals(type) || SysDefine.LOGIN.equals(type) || SysDefine.TERMSTATUS.equals(type) || SysDefine.CARSTATUS.equals(type)) {
            	stateKV.put(SysDefine.ISONLINE, "1");
            	if (SysDefine.LOGIN.equals(type) ) {
            			if( stateKV.containsKey(ProtocolItem.LOGOUT_SEQ)
            					|| stateKV.containsKey(ProtocolItem.LOGOUT_TIME)) {
            				
            				stateKV.put(SysDefine.ISONLINE, "0");
            				stateKV.put(ProtocolItem.REG_TYPE, "2");
            			} else {
            				stateKV.put(ProtocolItem.REG_TYPE, "1");
            			}
				}
            }
            if (SysDefine.LINKSTATUS.equals(type)) { // 车辆链接状态 TYPE：1上线，2心跳，3离线
                Map<String, String> linkmap = new TreeMap<String, String>();
                if ("1".equals(stateKV.get("TYPE"))
                		||"2".equals(stateKV.get("TYPE"))) {
                    linkmap.put(SysDefine.ISONLINE, "1");
                } else if ("3".equals(stateKV.get("TYPE"))) {
                    linkmap.put(SysDefine.ISONLINE, "0");
                    linkmap.put(SysDefine.ISALARM, "0");
                }
                linkmap.put(SysDefine.ONLINEUTC,now + ""); // 增加utc字段，插入系统时间
            }
            String vid = stateKV.get(SysDefine.VID);
            if (SysDefine.HISTORYDATA.equals(type)) {//补发历史原始数据存储
//            	sendMessages(SysDefine.SUPPLY_GROUP,null,vid,stateKV,true);
            	return;
            }
            
            // 时间加入map
            if (SysDefine.REALTIME.equals(type)) {
                stateKV.put(SysDefine.TIME, stateKV.get("2000"));
            } else if (SysDefine.LOGIN.equals(type)) {
            	if (stateKV.containsKey(ProtocolItem.LOGIN_TIME)) {
					stateKV.put(SysDefine.TIME, stateKV.get(ProtocolItem.LOGIN_TIME));
				} else if (stateKV.containsKey(ProtocolItem.LOGOUT_TIME)){
					stateKV.put(SysDefine.TIME, stateKV.get(ProtocolItem.LOGOUT_TIME));
				} else {
					stateKV.put(SysDefine.TIME, stateKV.get("1001"));
				}
            } else if (SysDefine.TERMSTATUS.equals(type)) {
                stateKV.put(SysDefine.TIME, stateKV.get("3101"));
            } else if (SysDefine.HISTORYDATA.equals(type)) {
                stateKV.put(SysDefine.TIME, stateKV.get("2000"));
            } else if (SysDefine.CARSTATUS.equals(type)) {
                stateKV.put(SysDefine.TIME, stateKV.get("3201"));
            } else if (SysDefine.RENTCAR.equals(type)) { // 租赁数据
                stateKV.put(SysDefine.TIME, stateKV.get("4001"));
            } else if (SysDefine.CHARGE.equals(type)) { // 充电设施数据
                stateKV.put(SysDefine.TIME, stateKV.get("4101"));
            } 

            stateKV.put(SysDefine.ONLINEUTC, now + ""); // 增加utc字段，插入系统时间
            try {
            	if (SysDefine.REALTIME.equals(type)
            			|| SysDefine.HISTORYDATA.equals(type))
            		processValid(stateKV);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
            Map<String, String> stateNewKV = stateKV; // 状态键值
            if (SysDefine.LINKSTATUS.equals(type)
        			|| SysDefine.LOGIN.equals(type)
        			|| SysDefine.REALTIME.equals(type)
        			|| SysDefine.TERMSTATUS.equals(type)){
            	stateNewKV = new TreeMap<String, String>();
            	stateNewKV.putAll(stateKV);
            }
            try {
            	if (SysDefine.LINKSTATUS.equals(type) 
            			|| SysDefine.LOGIN.equals(type) 
            			|| SysDefine.TERMSTATUS.equals(type) 
            			|| SysDefine.CARSTATUS.equals(type)) {
            		sendMessages(SysDefine.SYNES_GROUP,null,vid,stateNewKV,true);
            	}
            	if (SysDefine.REALTIME.equals(type)){
            		sendMessages(SysDefine.FENCE_GROUP,null,vid,stateNewKV,true);
//            		sendMessages(SysDefine.YAACTION_GROUP,null,vid,stateKV,true);
            	}
            	if (SysDefine.REALTIME.equals(type)
            			|| SysDefine.LINKSTATUS.equals(type) 
            			|| SysDefine.LOGIN.equals(type) 
            			|| SysDefine.TERMSTATUS.equals(type) 
            			|| SysDefine.CARSTATUS.equals(type)){
            		sendMessages(SysDefine.CUS_NOTICE_GROUP,null,vid,stateNewKV,true);
            	}
            	if (SysDefine.REALTIME.equals(type)
            			|| SysDefine.LINKSTATUS.equals(type)
            			|| SysDefine.LOGIN.equals(type)){
            		sendMessages(SysDefine.SPLIT_GROUP,null,vid,stateKV,true);
            	}
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    //synchronized
    private void sendMessages(String define,String topic,String field1, Object field2,boolean iscotn) {
    	if(iscotn)
    		collector.emit(define, new Values(field1, field2));
    	else
    		collector.emit(define, new Values(topic, field1, field2));
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

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    private Map<String, String> processValid(Map<String, String> dataMap) throws Exception {
        if(ObjectUtils.isNullOrEmpty(dataMap))
        	return null;
        
    	String vid = dataMap.get("VID");
        try {
            // 判断处理实时里程数据
            if (dataMap.containsKey("2202") && !"".equals(dataMap.get("2202"))) {

                long mileage = Long.parseLong(NumberUtils.stringNumber(dataMap.get("2202")));
                Long maxCacheMileage = maxMileMap.get(vid);
                Long minCacheMileage = minMileMap.get(vid);
                maxCacheMileage = ObjectUtils.isNullOrEmpty(maxCacheMileage) ?0L:maxCacheMileage;
                minCacheMileage = ObjectUtils.isNullOrEmpty(minCacheMileage) ?0L:minCacheMileage;
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

        try {
            // 充放电状态 1充电，2放电
            if (dataMap.get(SysDefine.MESSAGETYPE).equals(SysDefine.REALTIME)) {
                if (dataMap.containsKey("2301") && !"".equals(dataMap.get("2301"))) {
                    String status = dataMap.get("2301");
                    if (chargeMap.get(vid) == null) 
                    	chargeMap.put(vid, 0);
                    
                    int cacheNum = chargeMap.get(vid);
                    if ("1".equals(status) && "0".equals(NumberUtils.stringNumber(dataMap.get("2201")))) {
                    	chargeMap.put(vid, cacheNum + 1);
                        if (cacheNum >= 10) {
                            dataMap.put(SysDefine.ISCHARGE, "1");
                        }
                    } else {
                        dataMap.put(SysDefine.ISCHARGE, "0");
                        chargeMap.put(vid, 0);
                    }
                } else {
                    dataMap.put(SysDefine.ISCHARGE, "0");
                    chargeMap.put(vid, 0);
                }
            }
        } catch (Exception e) {
            System.out.println("----判断是否充电异常！" + e);
        }
        //向最后的数据中加入车辆当前所在行政区域id
        /*try {
			if (dataMap.containsKey(ProtocolItem.longitude)
					&& dataMap.containsKey(ProtocolItem.latitude)) {
				
				String longit = dataMap.get(ProtocolItem.longitude);
				String latit = dataMap.get(ProtocolItem.latitude);
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
        // 动力蓄电池报警标志
		/*
		 * 1：温度差异报警；0：正常 1：电池极柱高温报警；0：正常 1：动力蓄电池包过压报警；0：正常 1：动力蓄电池包欠压报警；0：正常 1：SOC低报警；0：正常 1：单体蓄电池过压报警；0：正常 1：单体蓄电池欠压报警；0：正常 1：SOC太低报警；0：正常 1：SOC过高报警；0：正常 1：动力蓄电池包不匹配报警；0：正常 1：动力蓄电池一致性差报警；0：正常 1：绝缘故障；0：正常
		 */
        try {
            if (dataMap.containsKey("2801") && !"".equals(dataMap.get("2801"))) {
                String binaryStr = TimeUtils.fillNBitBefore(Long.toBinaryString(Long.parseLong(NumberUtils.stringNumber(dataMap.get("2801")))), 12, "0");
                dataMap.put("2912", new String(binaryStr.substring(0, 1)));
                dataMap.put("2911", new String(binaryStr.substring(1, 2)));
                dataMap.put("2910", new String(binaryStr.substring(2, 3)));
                dataMap.put("2909", new String(binaryStr.substring(3, 4)));
                dataMap.put("2908", new String(binaryStr.substring(4, 5)));
                dataMap.put("2907", new String(binaryStr.substring(5, 6)));
                dataMap.put("2906", new String(binaryStr.substring(6, 7)));
                dataMap.put("2905", new String(binaryStr.substring(7, 8)));
                dataMap.put("2904", new String(binaryStr.substring(8, 9)));
                dataMap.put("2903", new String(binaryStr.substring(9, 10)));
                dataMap.put("2902", new String(binaryStr.substring(10, 11)));
                dataMap.put("2901", new String(binaryStr.substring(11, 12)));
                binaryStr=null;
            }else if(dataMap.containsKey("3801") && !"".equals(dataMap.get("3801"))) {
                String binaryStr = TimeUtils.fillNBitBefore(Long.toBinaryString(Long.parseLong(NumberUtils.stringNumber(dataMap.get("3801")))), 32, "0");
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
                binaryStr=null;
            }
        } catch (Exception e) {
            System.out.println("----动力蓄电池报警标志处理异常！" + e);
        }
        // 车载终端状态3110
		/*
		 * 1：通电；0：断开 BIT 3102 1：电源正常；0：电源异常 BIT 3103 1：通信传输正常；0：通信传输异常 BIT 3104 其他异常，1：正常；0：异常 BIT 3105
		 */
        try {
            if (dataMap.containsKey("3110") && !"".equals(dataMap.get("3110"))) {
                String binaryStr = TimeUtils.fillNBitBefore(Long.toBinaryString(Long.parseLong(NumberUtils.stringNumber(dataMap.get("3110")))), 4, "0");
                dataMap.put("3105", new String(binaryStr.substring(0, 1)));
                dataMap.put("3104", new String(binaryStr.substring(1, 2)));
                dataMap.put("3103", new String(binaryStr.substring(2, 3)));
                dataMap.put("3102", new String(binaryStr.substring(3, 4)));
                binaryStr=null;
            }
        } catch (Exception e) {
            System.out.println("----车载终端状态标志处理异常！" + e);
        }
        
        return dataMap;
    }
    
    private void generalAlarmToFaultCode(Map<String, String> dat,String binaryStr){
    	
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
        dat.put("380119", unit19);
        dat.put("380120", unit20);
        dat.put("380121", unit21);
        dat.put("380122", unit22);
        dat.put("380123", unit23);
        dat.put("380124", unit24);
        dat.put("380125", unit25);
        dat.put("380126", unit26);
        dat.put("380127", unit27);
        dat.put("380128", unit28);
        dat.put("380129", unit29);
        dat.put("380130", unit30);
        dat.put("380131", unit31);
        
        StringBuffer sb = new StringBuffer();
        
        sb.append("1".equals(unit19)?"38011901":"38011900").append("|")
        .append("1".equals(unit20)?"38012001":"38012000").append("|")
        .append("1".equals(unit21)?"38012101":"38012100").append("|")
        .append("1".equals(unit22)?"38012201":"38012200").append("|")
        .append("1".equals(unit23)?"38012301":"38012300").append("|")
        .append("1".equals(unit24)?"38012401":"38012400").append("|")
        .append("1".equals(unit25)?"38012501":"38012500").append("|")
        .append("1".equals(unit26)?"38012601":"38012600").append("|")
        .append("1".equals(unit27)?"38012701":"38012700").append("|")
        .append("1".equals(unit28)?"38012801":"38012800").append("|")
        .append("1".equals(unit29)?"38012901":"38012900").append("|")
        .append("1".equals(unit30)?"38013001":"38013000").append("|")
        .append("1".equals(unit31)?"38013101":"38013100");
        
        String faultCode2809 = dat.get("2809");
        if (null != faultCode2809 && ! "".equals(faultCode2809.trim())) {
        	sb.append("|").append(faultCode2809);
        }
        dat.put("2809", sb.toString());
        
//        .append("1".equals(unit00)?"38010001":"38010000")
//        .append("1".equals(unit01)?"38010101":"38010100")
//        .append("1".equals(unit02)?"38010201":"38010200")
//        .append("1".equals(unit03)?"38010301":"38010300")
//        .append("1".equals(unit04)?"38010401":"38010400")
//        .append("1".equals(unit05)?"38010501":"38010500")
//        .append("1".equals(unit06)?"38010601":"38010600");
    }
}
