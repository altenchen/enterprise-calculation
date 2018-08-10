package storm.bolt.deal.norm;

import org.apache.commons.lang.math.NumberUtils;
import storm.util.ConfigUtils;
import org.apache.commons.collections.MapUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.protocol.*;
import storm.stream.CusNoticeGroupStream;
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
    private static final Logger logger = LoggerFactory.getLogger(FilterBolt.class);
    private static final ConfigUtils configUtils = ConfigUtils.getInstance();
    private static final ParamsRedisUtil paramsRedisUtil = ParamsRedisUtil.getInstance();
    private static final CusNoticeGroupStream CUS_NOTICE_GROUP_STREAM = new CusNoticeGroupStream();
    private OutputCollector collector;
    private CusNoticeGroupStream.Emiter cusNoticeGroupStreamEmiter;
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
        cusNoticeGroupStreamEmiter = CUS_NOTICE_GROUP_STREAM.buildStreamEmiter(collector);

        maxMileMap = new HashMap<String, Long>();
        minMileMap = new HashMap<String, Long>();
//        lastMile = new HashMap<String, Integer>();
//        lastgpsDatMile = new HashMap<String, Integer>();
//        lastgps = new HashMap<String, double[]>();
//        lastgpsRegion = new HashMap<String, String>();
        chargeMap = new HashMap<String, Integer>();

        rebootTime = System.currentTimeMillis();
        String nocheckObj = configUtils.sysDefine.getProperty("sys.reboot.nocheck");
        if (!StringUtils.isEmpty(nocheckObj)) {
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

            Map<String, String> stateKV = new TreeMap<>(); // 状态键值
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
                stateKV.put(SysDefine.IS_ONLINE, "1");

                // 如果是终端注册报文
                if (CommandType.SUBMIT_LOGIN.equals(type) ) {
                    // 如果stateKV包含登出流水号或者登出时间, 则设置在线状态(10002)为"0"
                    // 并且将`平台注册通知类型`设置为`车机离线`
                    // 否则将`平台注册通知类型`设置为`车机终端上线`
                    if(stateKV.containsKey(SUBMIT_LOGIN.LOGOUT_SEQ)
                        || stateKV.containsKey(SUBMIT_LOGIN.LOGOUT_TIME)) {

                        stateKV.put(SysDefine.IS_ONLINE, "0");
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
//                        || SUBMIT_LINKSTATUS.isHeartbeatNotice(stateKV.get(SUBMIT_LINKSTATUS.LINK_TYPE))) {
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
//                sendMessages(SysDefine.SUPPLY_GROUP,null,vid,stateKV,true);
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
                stateKV.put(SysDefine.TIME, stateKV.get(DataKey._2000_COLLECT_TIME));
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
                        || CommandType.SUBMIT_HISTORY.equals(type)) {
                    processValid(stateKV);
                }
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

            String vid = stateKV.get(DataKey.VEHICLE_ID);
            if(paramsRedisUtil.isTraceVehicleId(vid)) {
                logger.warn("VID[" + vid + "]数据预处理完成");
            }
            try {
                if (CommandType.SUBMIT_LINKSTATUS.equals(type)
                        || CommandType.SUBMIT_LOGIN.equals(type)
                        || CommandType.SUBMIT_TERMSTATUS.equals(type)
                        || CommandType.SUBMIT_CARSTATUS.equals(type)) {
                    // consumer: ES数据同步处理
                    sendMessages(SysDefine.SYNES_GROUP, vid,stateNewKV);
                }
                if (CommandType.SUBMIT_REALTIME.equals(type)){
                    // consumer: 电子围栏告警处理
                    sendMessages(SysDefine.FENCE_GROUP, vid,stateNewKV);
                    // consumer: 雅安用户行为处理
//                    sendMessages(SysDefine.YAACTION_GROUP,null,vid,stateKV,true);
                }
                if (CommandType.SUBMIT_REALTIME.equals(type)
                        || CommandType.SUBMIT_LINKSTATUS.equals(type)
                        || CommandType.SUBMIT_LOGIN.equals(type)
                        || CommandType.SUBMIT_TERMSTATUS.equals(type)
                        || CommandType.SUBMIT_CARSTATUS.equals(type)){
                    // consumer: SOC与超时处理
                    cusNoticeGroupStreamEmiter.emit(vid, stateNewKV);
                }
                if (CommandType.SUBMIT_REALTIME.equals(type)
                        || CommandType.SUBMIT_LINKSTATUS.equals(type)
                        || CommandType.SUBMIT_LOGIN.equals(type)){
                    // 预警处理
                    sendMessages(SysDefine.SPLIT_GROUP, vid,stateKV);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    private void sendMessages(String streamId, String vid, Map<String, String> data) {
        collector.emit(streamId, new Values(vid, data));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SysDefine.SPLIT_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
        declarer.declareStream(SysDefine.FENCE_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
        declarer.declareStream(SysDefine.YAACTION_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
        declarer.declareStream(SysDefine.SYNES_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
        CUS_NOTICE_GROUP_STREAM.declareStream(declarer);
        declarer.declareStream(SysDefine.HISTORY, new Fields("TOPIC", DataKey.VEHICLE_ID, "VALUE"));
    }

    @Nullable
    private void processValid(@NotNull final Map<String, String> dataMap) {
        if(MapUtils.isEmpty(dataMap)) {
            return;
        }
        
        String vid = dataMap.get(DataKey.VEHICLE_ID);

        // region 更新所有车的最大里程数和最小里程数, 并将差值发往下游.
        try {
            // 判断处理实时里程数据
            if (dataMap.containsKey(DataKey._2202_TOTAL_MILEAGE)
                && !"".equals(dataMap.get(DataKey._2202_TOTAL_MILEAGE))) {

                String str = dataMap.get(DataKey._2202_TOTAL_MILEAGE);
                long mileage = Long.parseLong(
                    org.apache.commons.lang.math.NumberUtils.isNumber(str) ? str : "0"
                );
                Long maxCacheMileage = maxMileMap.get(vid);
                Long minCacheMileage = minMileMap.get(vid);
                maxCacheMileage = null == maxCacheMileage ? 0L : maxCacheMileage;
                minCacheMileage = null == minCacheMileage ? 0L : minCacheMileage;
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
        } catch (Exception e) {
            logger.warn("预处理里程!", e);
        }
        // endregion

        // region 连续10次处于充电状态并且车速为0, 则记录充电状态为"1", 否则重置次数为0.
        try {
            // 充放电状态 1充电，2放电
            final String charging = "1";
            final String disCharging = "2";
            if (CommandType.SUBMIT_REALTIME.equals(dataMap.get(SysDefine.MESSAGETYPE))) {
                if (dataMap.containsKey(DataKey._2301_CHARGE_STATUS)
                    && !StringUtils.isEmpty(dataMap.get(DataKey._2301_CHARGE_STATUS))) {
                    String status = dataMap.get(DataKey._2301_CHARGE_STATUS);
                    if (chargeMap.get(vid) == null) {
                        chargeMap.put(vid, 0);
                    }
                    
                    int cacheNum = chargeMap.get(vid);
                    final String str = dataMap.get(DataKey._2201_SPEED);
                    if (charging.equals(status) &&
                        // 车速为0
                        "0".equals(NumberUtils.isNumber(str) ? str : "0")) {
                        chargeMap.put(vid, cacheNum + 1);
                        if (cacheNum >= 10) {
                            dataMap.put(SysDefine.IS_CHARGE, "1");
                        }
                    } else {
                        dataMap.put(SysDefine.IS_CHARGE, "0");
                        chargeMap.put(vid, 0);
                    }
                } else {
                    // 如果不包含充电状态, 则记录充电状态为"0"
                    dataMap.put(SysDefine.IS_CHARGE, "0");
                    chargeMap.put(vid, 0);
                }
            }
        } catch (Exception e) {
            logger.warn("判断是否充电异常!", e);
        }
        // endregion

        //  region 动力蓄电池报警标志
        /*
         * 1：温度差异报警；0：正常 1：电池极柱高温报警；0：正常 1：动力蓄电池包过压报警；0：正常 1：动力蓄电池包欠压报警；0：正常 1：SOC低报警；0：正常 1：单体蓄电池过压报警；0：正常 1：单体蓄电池欠压报警；0：正常 1：SOC太低报警；0：正常 1：SOC过高报警；0：正常 1：动力蓄电池包不匹配报警；0：正常 1：动力蓄电池一致性差报警；0：正常 1：绝缘故障；0：正常
         */
        try {
            // region 北京地标: 动力蓄电池报警标志解析存储, 见表20
            if (dataMap.containsKey(DataKey._2801_POWER_BATTERY_ALARM_FLAG_2801)
                && !"".equals(dataMap.get(DataKey._2801_POWER_BATTERY_ALARM_FLAG_2801))) {
                // WORD通过二进制标识(预留部分填0)
                String str = dataMap.get(DataKey._2801_POWER_BATTERY_ALARM_FLAG_2801);
                String binaryStr = TimeUtils.fillNBitBefore(
                    Long.toBinaryString(
                        Long.parseLong(
                            org.apache.commons.lang.math.NumberUtils.isNumber(str) ? str : "0"
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
            else if(dataMap.containsKey(DataKey._3801_ALARM_MARK) && !"".equals(dataMap.get(DataKey._3801_ALARM_MARK))) {
                String str = dataMap.get(DataKey._3801_ALARM_MARK);
                String binaryStr = TimeUtils.fillNBitBefore(
                    Long.toBinaryString(
                        Long.parseLong(
                            NumberUtils.isNumber(str) ? str : "0"
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
            }
            // endregion
        } catch (Exception e) {
            logger.warn("动力蓄电池报警标志处理异常!", e);
        }
        // endregion

        // region 北京地标: 车载终端状态解析存储, 见表23
        // 车载终端状态3110
        /*
         * 1：通电；0：断开 BIT 3102
         * 1：电源正常；0：电源异常 BIT 3103
         * 1：通信传输正常；0：通信传输异常 BIT 3104
          * 其他异常，1：正常；0：异常 BIT 3105
         */
        try {
            if (dataMap.containsKey(DataKey._3110_STATUS_FLAGS) && !"".equals(dataMap.get(DataKey._3110_STATUS_FLAGS))) {
                String str = dataMap.get(DataKey._3110_STATUS_FLAGS);
                String binaryStr = TimeUtils.fillNBitBefore(
                    Long.toBinaryString(
                        Long.parseLong(
                            org.apache.commons.lang.math.NumberUtils.isNumber(str) ? str : "0"
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
            logger.warn("车载终端状态标志处理异常!", e);
        }
        // endregion
    }
}
