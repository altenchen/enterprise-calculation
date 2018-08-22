package storm.bolt.deal.norm;

import com.google.common.collect.Maps;
import com.sun.jersey.core.util.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;
import storm.dto.alarm.CoefOffset;
import storm.dto.alarm.CoefOffsetGetter;
import storm.dto.alarm.EarlyWarn;
import storm.dto.alarm.EarlyWarnsGetter;
import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LINKSTATUS;
import storm.protocol.SUBMIT_LOGIN;
import storm.stream.KafkaStream;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.JsonUtils;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author wza
 * 预警处理
 */
public class AlarmBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1720001L;

    private static final Logger LOG = LoggerFactory.getLogger(AlarmBolt.class);

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private OutputCollector collector;

    private Map<String, List<String>> filterMap = Maps.newHashMap();

    /**
     * 连续多少条报警才发送通知
     */
    private static int alarmNum = 10;

    /**
     * 告警消息 kafka 输出 topic
     */
    private String vehAlarmTopic;
    /**
     * HBase 车辆报警状态存储 kafka 输出 topic
     */
    private String vehAlarmStoreTopic;
    private Map<String, String> alarmMap = Maps.newHashMap();
    private Map<String, String> vehDataMap = Maps.newHashMap();

    /**
     * 车辆报警信息缓存(vid----是否报警_最后报警时间)
     */
    private Map<String, String> vid2Alarm = Maps.newHashMap();
    /**
     * 车辆报警信息缓存(vid----报警结束次数)
     */
    private Map<String, String> vid2AlarmEnd = Maps.newHashMap();
    private Map<String, String> vid2AlarmInfo = Maps.newHashMap();
    private Map<String, Set<String>> vidAlarmIds = Maps.newHashMap();
    private Map<String, Map<String, String>> lastCache = Maps.newHashMap();

    /**
     * 默认300秒同步数据库新建规则
     */
    private long flushtime = 300;

    /**
     * 每隔多少时间推送ES一次,默认一分钟，60秒。如果负数或者0代表实时推送;
     */
    private static long oncesend = 60;

    /**
     * 离线超时时间
     */
    private static long onlinetime = 180 * 1000;

    /**
     *
     */
    private int buffsize = 5000000;

    /**
     * 车辆vid队列
     * LinkedBlockingQueue是线程安全你的阻塞队列
     */
    private Queue<String> alives = new LinkedBlockingQueue<>(buffsize);

    /**
     *
     */
    private Set<String> aliveSet = new HashSet<>(buffsize / 5);

    /**
     *
     */
    private LinkedBlockingQueue<String> needListenAlarms = new LinkedBlockingQueue<>(buffsize);

    /**
     *
     */
    private Set<String> needListenAlarmSet = new HashSet<>(buffsize / 5);

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        vehAlarmTopic = stormConf.get(SysDefine.KAFKA_TOPIC_ALARM).toString();
        vehAlarmStoreTopic = stormConf.get(SysDefine.KAFKA_TOPIC_ALARM_STORE).toString();

        try {
            Object alarmObject = stormConf.get(SysDefine.ALARM_CONTINUE_COUNTS);
            if (null != alarmObject) {
                String str = alarmObject.toString();
                alarmNum = NumberUtils.toInt(str, 0);
            }
            Object oncetime = stormConf.get(SysDefine.ES_SEND_TIME);
            if (null != oncetime) {
                oncesend = Long.valueOf(oncetime.toString());
            }
            flushtime = Long.parseLong(stormConf.get(SysDefine.DB_CACHE_FLUSH_TIME_SECOND).toString());

            Object offli = stormConf.get(StormConfigKey.REDIS_OFFLINE_SECOND);
            if (null != offli) {
                onlinetime = 1000 * Long.valueOf(offli.toString());
            }
        } catch (Exception e) {
            LOG.warn("初始化配置异常", e);
        }

        try {

            class AllSendClass implements Runnable {

                @Override
                public void run() {
                    int count = 0;

                    try {
                        if (alives.size() > 0) {

                            // 将alives中所有vid出队, 如果存在对应车辆的最后一帧数据, 则发送到 ElasticSearch.
                            String vid = alives.poll();
                            while (null != vid) {
                                aliveSet.remove(vid);

                                // 车辆最后一帧数据
                                final Map<String, String> data = lastCache.get(vid);
                                if (MapUtils.isNotEmpty(data)) {

                                    final String lastUtc = data.get(SysDefine.ONLINE_UTC);
                                    if (StringUtils.isNotBlank(lastUtc)) {

                                        sendToNext(vid, data);

                                        // 每发送1000条, 睡1秒钟.
                                        count++;
                                        if (count % 1000 == 0) {
                                            count = 1;
                                            TimeUnit.MILLISECONDS.sleep(1);
                                        }
                                    }
                                }
                                vid = alives.poll();
                            }
                        }

                    } catch (Exception e) {
                        LOG.warn("AllSendClass:", e);
                    }
                }

            }

            // TODO: 使用storm心跳帧来取代
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new AllSendClass(), 0, oncesend, TimeUnit.SECONDS);

            class RebulidClass implements Runnable {

                @Override
                public void run() {
                    try {
                        EarlyWarnsGetter.rebulid();
                        CoefOffsetGetter.rebuild();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }

            // TODO: 使用storm心跳帧来取代
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new RebulidClass(), 0, flushtime, TimeUnit.SECONDS);

            class TimeOutClass implements Runnable {

                @Override
                public void run() {
                    try {
                        timeOutOver(onlinetime);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
            // TODO: 使用storm心跳帧来取代
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new TimeOutClass(), 0, flushtime, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("启动定时任务异常", e);
        }
    }

    @Override
    public void execute(@NotNull final Tuple input) {
        if (input.getSourceStreamId().equals(SysDefine.SPLIT_GROUP)) {
            final String vid = input.getString(0);
            final Map<String, String> data = (Map<String, String>) input.getValue(1);

            if (!data.containsKey(DataKey.TIME)
                || StringUtils.isEmpty(data.get(DataKey.TIME))) {
                return;
            }
            final String type = data.get(DataKey.MESSAGE_TYPE);

            final String linkType = data.get(SUBMIT_LINKSTATUS.LINK_TYPE);
            if (
                // 实时信息上报
                CommandType.SUBMIT_REALTIME.equals(type)
                    || (
                    // 链接状态通知
                    CommandType.SUBMIT_LINKSTATUS.equals(type)
                        // 是否离线通知
                        && SUBMIT_LINKSTATUS.isOfflineNotice(
                        linkType
                    )
                )
                    || (
                    // 终端注册消息
                    CommandType.SUBMIT_LOGIN.equals(type)
                        && (
                        // 登出流水号
                        data.containsKey(SUBMIT_LOGIN.LOGOUT_SEQ)
                            // 登出时间
                            || data.containsKey(SUBMIT_LOGIN.LOGOUT_TIME)))
            ) {
                try {
                    processAlarm(data, type);
                } catch (Exception e) {
                    LOG.warn("软报警分析出错! [{}]", data);
                }
            }

            if (
                // 实时信息上报
                CommandType.SUBMIT_REALTIME.equals(type)
                    // 终端注册消息
                    || CommandType.SUBMIT_LOGIN.equals(type)
                    // 状态信息上报
                    || CommandType.SUBMIT_TERMSTATUS.equals(type)
                    // 车辆运行状态
                    || CommandType.SUBMIT_CARSTATUS.equals(type)) {

                // 更新告警状态和
                try {
                    // 车辆报警信息缓存(vid----是否报警_最后报警时间)
                    final String string = vid2Alarm.get(vid);
                    if (!StringUtils.isEmpty(string)) {
                        String[] alarmStr = string.split("_", 3);
                        data.put(SysDefine.IS_ALARM, alarmStr[0]);
                        data.put(SysDefine.ALARMUTC, alarmStr[1]);
                    }

                } catch (Exception e) {
                    LOG.warn("实时数据redis存储出错! [{}]", data);
                }
            }
            // 车辆链接状态帧, 更新上下线状态和告警状态
            else if (CommandType.SUBMIT_LINKSTATUS.equals(type)) {
                final Map<String, String> linkmap = Maps.newTreeMap();

                // 上线
                if (SUBMIT_LINKSTATUS.isOnlineNotice(linkType)) {
                    linkmap.put(DataKey._10002_IS_ONLINE, "1");
                }
                // 离线
                else if (SUBMIT_LINKSTATUS.isOfflineNotice(linkType)) {
                    linkmap.put(DataKey._10002_IS_ONLINE, "0");
                    linkmap.put(SysDefine.IS_ALARM, "0");
                }
                // 增加utc字段，插入系统时间
                linkmap.put(SysDefine.ONLINE_UTC, System.currentTimeMillis() + "");

                data.putAll(linkmap);
            }

            if (
                // 实时信息上报
                CommandType.SUBMIT_REALTIME.equals(type)
                    // 终端注册消息
                    || CommandType.SUBMIT_LOGIN.equals(type)
                    // 链接状态通知
                    || CommandType.SUBMIT_LINKSTATUS.equals(type)
                    // 状态信息上报
                    || CommandType.SUBMIT_TERMSTATUS.equals(type)
                    // 车辆运行状态
                    || CommandType.SUBMIT_CARSTATUS.equals(type)) {

                // 缓存最后一帧
                lastCache.put(vid, data);

                // 标记在线车辆vid, 并将vid添加带在线vid队列中, 用于 AllSendClass
                if (!aliveSet.contains(vid)) {
                    aliveSet.add(vid);
                    alives.offer(vid);
                }

                //实时发送(不缓存)到 实时含告警的报文信息 到es同步服务
                //sendToNext(SysDefine.SYNES_GROUP,vid, dat);
            }
            if (CommandType.SUBMIT_REALTIME.equals(type)) {
                try {
                    String veh2000 = data.get("2000");
                    if (!StringUtils.isEmpty(veh2000)) {
                        String string = vehDataMap.get(vid);
                        if (null == string || (string.compareTo(veh2000) < 0)) {
                            vehDataMap.put(vid, veh2000);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("Receive unknown kafka message-------------------StreamID:" + input.getSourceStreamId());
        }
    }

    @Override
    public void declareOutputFields(@NotNull final OutputFieldsDeclarer declarer) {

        KafkaStream.declareOutputFields(declarer, SysDefine.VEH_ALARM);
        KafkaStream.declareOutputFields(declarer, SysDefine.VEH_ALARM_REALINFO_STORE);

        declarer.declareStream(SysDefine.FAULT_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
        declarer.declareStream(SysDefine.SYNES_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * 软报警处理
     */
    private void processAlarm(Map<String, String> dataMap, String type) {
        if (MapUtils.isEmpty(dataMap)) {
            return;
        }
        String vid = dataMap.get(DataKey.VEHICLE_ID);
        String vType = dataMap.get("VTYPE");
        if (StringUtils.isEmpty(vid)
            || StringUtils.isEmpty(vType)) {
            return;
        }


        if (CommandType.SUBMIT_LINKSTATUS.equals(type)
            || CommandType.SUBMIT_LOGIN.equals(type)) {
            try {
                sendOverAlarmMessage(vid);
            } catch (Exception e) {
                System.out.println("---自动发送结束报警异常！" + e);
            }
            return;
        }

        List<EarlyWarn> warns = EarlyWarnsGetter.allWarnArrsByType(vType);
        if (CollectionUtils.isEmpty(warns)) {
            return;
        }
        try {
            int len = warns.size();
            for (int i = 0; i < len; i++) {
                EarlyWarn warn = warns.get(i);
                if (null == warn) {
                    continue;
                }

                int ret;
                if (null == warn.dependId) {

                    ret = processSingleAlarm(vid, warn, dataMap);
                    sendAlarmMessage(ret, vid, warn, dataMap);

                } else {
                    EarlyWarn warndepend = EarlyWarnsGetter.getEarlyByDependId(warn.dependId);
                    ret = processSingleAlarm(vid, warn, dataMap);
                    if (ret == 1) {

                        //先判断父级约束是否成立，如果成立则继续判断子级约束

                        if (null != warndepend) {
                            ret = processSingleAlarm(vid, warndepend, dataMap);
                        }
                    }
                    sendAlarmMessage(ret, vid, warndepend, dataMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 车辆发离线通知，系统自动发送结束报警通知
     *
     * @param vid
     */
    private void sendOverAlarmMessage(String vid) {
        if (filterMap.containsKey(vid)) {
            String time = vid2Alarm.get(vid).split("_")[2];
            List<String> list = filterMap.get(vid);
            for (String filterId : list) {
                //上条报警，本条不报警，说明是结束报警，发送结束报警报文
                String alarmId = alarmMap.get(vid + "#" + filterId);
                EarlyWarn warn = EarlyWarnsGetter.getEarlyByDependId(filterId);
                if (null == warn) {
                    continue;
                }

                String alarmName = warn.name;
                int alarmLevel = warn.levels;
                String left1 = warn.left1DataItem;

                //String alarmEnd = "VEHICLE_ID:"+vid+",ALARM_ID:"+alarmId+",STATUS:3,TIME:"+time+",CONST_ID:"+filterId;
                Map<String, Object> sendMsg = new TreeMap<>();
                sendMsg.put(DataKey.VEHICLE_ID, vid);
                sendMsg.put("ALARM_ID", alarmId);
                sendMsg.put("STATUS", 3);
                sendMsg.put("TIME", time);
                sendMsg.put("CONST_ID", filterId);
                String alarmEnd = JSON_UTILS.toJson(sendMsg);

                sendMsg.put("ALARM_NAME", alarmName);
                sendMsg.put("ALARM_LEVEL", alarmLevel);
                sendMsg.put("LEFT1", left1);
                String alarmhbase = JSON_UTILS.toJson(sendMsg);
                //kafka存储
                sendAlarmKafka(SysDefine.VEH_ALARM, vehAlarmTopic, vid, alarmEnd);
                //hbase存储
                sendAlarmKafka(SysDefine.VEH_ALARM_REALINFO_STORE, vehAlarmStoreTopic, vid, alarmhbase);
                //redis存储
                saveRedis(vid, "0", time);
                alarmMap.remove(vid + "#" + filterId);
            }
            filterMap.remove(vid);
        }
        //离线重置所有报警约束
        removeByVid(vid);
    }

    private int processSingleAlarm(String vid, EarlyWarn warn, Map<String, String> dataMap) {
        int ret = 0;
        try {
            if (null != warn) {
                //左1数据项ID
                String left1 = warn.left1DataItem;
                //偏移系数，
                CoefOffset coefOffset = CoefOffsetGetter.getCoefOffset(left1);
                String left1Value = dataMap.get(left1);
                //上传的实时数据包含左1字段 才进行预警判定
                if (StringUtils.isEmpty(left1Value)) {
                    return ret;
                }
                boolean stringIsNum = org.apache.commons.lang.math.NumberUtils.isNumber(left1Value);

                if (null != coefOffset
                    && 0 == coefOffset.type
                    && !stringIsNum
                ) {
                    return ret;
                }
                if (null == coefOffset
                    && !stringIsNum) {
                    return ret;
                }

                int leftExp = Integer.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(warn.leftExpression) ? warn.leftExpression : "0");
                //左2数据项ID
                String left2 = warn.left2DataItem;
                int midExp = Integer.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(warn.middleExpression) ? warn.middleExpression : "0");
                double right1 = warn.right1Value;
                double right2 = warn.right2Value;
                //左二字段为空，L2_ID为空  根据EXPR_MID，和R1_VAL, R2_VAL判断
                if (StringUtils.isEmpty(left2)) {

                    //不需要处理偏移和系数
                    if (null == coefOffset) {
                        double left1_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(left1Value) ? left1Value : "0");
                        //判断是否软报警条件(true/false)
                        ret = diffMarkValid(left1_value, midExp, right1, right2);
                    } else if (0 == coefOffset.type) {
                        double left1_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(left1Value) ? left1Value : "0");
                        left1_value = (left1_value - coefOffset.offset) / coefOffset.coef;
                        //判断是否软报警条件(true/false)
                        ret = diffMarkValid(left1_value, midExp, right1, right2);
                    }
                    // 1代表是数据项是数组
                    else if (1 == coefOffset.type) {
                        //  判断:单体蓄电池电压值列表    7003 |温度值列表    7103
                        String[] arr = left1Value.split("\\|");
                        for (int i = 0; i < arr.length; i++) {
                            String arri = arr[i];
                            if (!StringUtils.isEmpty(arri)) {

                                String v = new String(Base64.decode(arri), "GBK");

                                if (v.contains(":")) {
                                    String[] arr2m = v.split(":");
                                    if (arr2m.length == 2
                                        && !StringUtils.isEmpty(arr2m[1])) {

                                        String[] arr2 = arr2m[1].split("_");
                                        for (int j = 0; j < arr2.length; j++) {
                                            double value = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(arr2[j]) ? arr2[j] : "0");
                                            value = (value - coefOffset.offset) / coefOffset.coef;
                                            //判断是否软报警条件(true/false)
                                            ret = diffMarkValid(value, midExp, right1, right2);
                                            if (ret == 1) {
                                                return ret;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }

                } else {

                    String left2Value = dataMap.get(left2);
                    if (StringUtils.isEmpty(left2Value)) {
                        return ret;
                    }

                    //L2_ID不为空， L1_ID  EXPR_LEFT  L2_ID
                    if (!left1.equals(left2)) {
                        if (null != coefOffset && 1 == coefOffset.type) {
                            return ret;
                        }

                        CoefOffset left2coefOffset = CoefOffsetGetter.getCoefOffset(left2);
                        if (null != left2coefOffset && 1 == left2coefOffset.type) {
                            return ret;
                        }

                        if (!org.apache.commons.lang.math.NumberUtils.isNumber(left1Value)
                            || !org.apache.commons.lang.math.NumberUtils.isNumber(left2Value)) {
                            return ret;
                        }

                        double left1_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(left1Value) ? left1Value : "0");
                        if (null != coefOffset) {
                            left1_value = (left1_value - coefOffset.offset) / coefOffset.coef;
                        }

                        double left2_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(left2Value) ? left2Value : "0");
                        if (null != left2coefOffset) {
                            left2_value = (left2_value - left2coefOffset.offset) / left2coefOffset.coef;
                        }

                        double left_value = diffMarkValid2(leftExp, left1_value, left2_value);

                        //判断是否软报警条件(true/false)
                        ret = diffMarkValid(left_value, midExp, right1, right2);
                        return ret;

                    } else {
                        String lastData = "";
                        Map<String, String> last = lastCache.get(vid);
                        if (null != last) {
                            lastData = last.get(left1);
                        }
                        //上传的实时数据包含左1字段
                        if (!StringUtils.isEmpty(lastData)) {

                            if ((left2Value.contains("|") && !lastData.contains("|"))
                                || (!left2Value.contains("|") && lastData.contains("|"))
                                || (left2Value.contains(":") && !lastData.contains(":"))
                                || (!left2Value.contains(":") && lastData.contains(":"))
                                || (left2Value.contains("_") && !lastData.contains("_"))
                                || (!left2Value.contains("_") && lastData.contains("_"))) {
                                return ret;
                            }

                            if (left2Value.contains("|")) {

                                String[] larr = lastData.split("\\|");
                                String[] arr = left2Value.split("\\|");

                                if (arr.length != larr.length) {
                                    return ret;
                                }

                                for (int i = 0; i < arr.length; i++) {
                                    String larri = larr[i];
                                    String arri = arr[i];
                                    if (!StringUtils.isEmpty(larri)
                                        && !StringUtils.isEmpty(arri)) {

                                        String lv = new String(Base64.decode(larri), "GBK");
                                        String v = new String(Base64.decode(arri), "GBK");

                                        if (lv.contains(":") && v.contains(":")) {
                                            String[] larr2m = lv.split(":");
                                            String[] arr2m = v.split(":");
                                            if (larr2m.length != arr2m.length) {
                                                return ret;
                                            }
                                            if (arr2m.length == 2
                                                && !StringUtils.isEmpty(larr2m[1])
                                                && !StringUtils.isEmpty(arr2m[1])) {

                                                String[] larr2 = larr2m[1].split("_");
                                                String[] arr2 = arr2m[1].split("_");
                                                if (larr2.length == arr2.length) {

                                                    for (int j = 0; j < arr2.length; j++) {
                                                        double left1_value = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(larr2[j]) ? larr2[j] : "0");
                                                        double left2_value = Double.parseDouble(org.apache.commons.lang.math.NumberUtils.isNumber(arr2[j]) ? arr2[j] : "0");
                                                        if (null != coefOffset) {
                                                            left1_value = (left1_value - coefOffset.offset) / coefOffset.coef;
                                                            left2_value = (left2_value - coefOffset.offset) / coefOffset.coef;
                                                        }
                                                        double left_value = diffMarkValid2(leftExp, left1_value, left2_value);
                                                        //判断是否软报警条件(true/false)
                                                        ret = diffMarkValid(left_value, midExp, right1, right2);
                                                        if (ret == 1) {
                                                            return ret;
                                                        }
                                                    }

                                                }
                                            }
                                        }
                                    } else {
                                        return ret;
                                    }
                                }

                                return ret;
                            }
                            double left1_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(lastData) ? lastData : "0");
                            double left2_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(left2Value) ? left2Value : "0");
                            if (null != coefOffset) {
                                left1_value = (left1_value - coefOffset.offset) / coefOffset.coef;
                                left2_value = (left2_value - coefOffset.offset) / coefOffset.coef;
                            }
                            double left_value = diffMarkValid2(leftExp, left1_value, left2_value);
                            //判断是否软报警条件(true/false)
                            ret = diffMarkValid(left_value, midExp, right1, right2);
                        }
                    }

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    private void sendAlarmMessage(int ret, String vid, EarlyWarn warn, Map<String, String> dataMap) {
        if (null == warn) {
            return;
        }
        String filterId = warn.id;
        String alarmName = warn.name;
        int alarmLevel = warn.levels;
        //左1数据项ID
        String left1 = warn.left1DataItem;
        //左2数据项ID
        String left2 = warn.left2DataItem;
        double right1 = warn.right1Value;
        double right2 = warn.right2Value;
        String alarmGb = dataMap.get(DataKey._2920_ALARM_STATUS);
        alarmGb = org.apache.commons.lang.math.NumberUtils.isNumber(alarmGb) ? alarmGb : "0";
        if (!"0".equals(alarmGb)) {
            int gbAlarm = Integer.parseInt(alarmGb);
            alarmLevel = Math.max(alarmLevel, gbAlarm);
        }
        String time = dataMap.get(DataKey.TIME);
        long alarmUtc = 0;
        try {
            alarmUtc = DateUtils.parseDate(time, new String[]{FormatConstant.DATE_FORMAT}).getTime();
        } catch (ParseException e) {
            LOG.warn("反序列化时间异常", e);
        }
        if (0 == alarmUtc) {
            alarmUtc = System.currentTimeMillis();
        }
        String vidFilterId = vid + "_" + filterId;
        List<String> list = filterMap.get(vid);
        if (ret == 1) {
            //报警缓存包含vid，且vid对应的list含有此约束id，也就是此类型的报警，就说明上一条已报警

            if (!CollectionUtils.isEmpty(list) && list.contains(filterId)) {
                //上条报警，本条也报警，说明是【报警进行中】，发送报警进行中报文

                //redis存储
                saveRedis(vid, "1", time);
                vid2AlarmInfo.put(vidFilterId, (alarmNum + 1) + "_0_" + alarmUtc);

                // region cache
                Set<String> infoIds = vidAlarmIds.computeIfAbsent(vid, k -> new HashSet<>());
                infoIds.add(vidFilterId);
                // endregion cache

            } else {
                //上条不报警，本条报警，说明是【开始报警】，发送开始报警报文

                String string = vid2AlarmInfo.get(vidFilterId);
                if (!StringUtils.isEmpty(string)) {
                    String[] infoArr = string.split("_", 3);
                    if (infoArr.length >= 3) {
                        int alarmNumThid = Integer.valueOf(infoArr[0]);
                        long alarmTime = Long.parseLong(infoArr[1]);
                        long lastAlarmUtc = Long.parseLong(infoArr[2]);
                        vid2AlarmInfo.put(vidFilterId, (alarmNumThid + 1) + "_" + alarmTime + "_" + lastAlarmUtc);

                        // region cache
                        Set<String> infoIds = vidAlarmIds.computeIfAbsent(vid, k -> new HashSet<>());
                        infoIds.add(vidFilterId);
                        // endregion cache

                        //**根据数据项的预警配置进行判断，条件成立针对最后预警发生时间或者个数累计进行进行判定，若超过3分钟或连续累计超过10次的条件方认为成立*//*
                        if (alarmNumThid + 1 >= alarmNum) {
                            String alarmId = vid + "_" + time + "_" + filterId;
                            alarmMap.put(vid + "#" + filterId, alarmId);
                            List<String> l = filterMap.get(vid);
                            if (null == l) {
                                l = new LinkedList<>();
                            }
                            l.add(filterId);
                            if (!needListenAlarmSet.contains(vid)) {
                                needListenAlarmSet.add(vid);
                                needListenAlarms.offer(vid);
                            }
                            filterMap.put(vid, l);

                            String lastAlarmUtcString;
                            try {
                                lastAlarmUtcString = DateFormatUtils.format(lastAlarmUtc, FormatConstant.DATE_FORMAT);
                            } catch (Exception e) {
                                LOG.warn("格式化时间异常", e);
                                lastAlarmUtcString = StringUtils.EMPTY;
                            }

                            Map<String, Object> sendMsg = new TreeMap<>();
                            sendMsg.put(DataKey.VEHICLE_ID, vid);
                            sendMsg.put("ALARM_ID", alarmId);
                            sendMsg.put("STATUS", 1);
                            sendMsg.put("TIME", lastAlarmUtcString);
                            sendMsg.put("CONST_ID", filterId);
                            sendMsg.put("ALARM_LEVEL", alarmLevel);
                            String alarmStart = JSON_UTILS.toJson(sendMsg);

                            sendMsg.put("ALARM_NAME", alarmName);
                            sendMsg.put("LEFT1", left1);
                            sendMsg.put("LEFT2", left2);
                            sendMsg.put("RIGHT1", right1);
                            sendMsg.put("RIGHT2", right2);
                            String alarmHbase = JSON_UTILS.toJson(sendMsg);
                            //发送kafka提供数据库存储
                            sendAlarmKafka(SysDefine.VEH_ALARM, vehAlarmTopic, vid, alarmStart);
                            //hbase存储
                            sendAlarmKafka(SysDefine.VEH_ALARM_REALINFO_STORE, vehAlarmStoreTopic, vid, alarmHbase);

                            sendMsg.put("UTC_TIME", lastAlarmUtc);
                            //redis存储
                            saveRedis(vid, "1", lastAlarmUtcString);
                            vid2AlarmInfo.put(vidFilterId, (alarmNumThid + 1) + "_" + alarmTime + "_" + alarmUtc);
                        }
                    }

                } else {
                    vid2AlarmInfo.put(vidFilterId, "1_0_" + alarmUtc);

                    // region cache
                    Set<String> infoIds = vidAlarmIds.computeIfAbsent(vid, k -> new HashSet<>());
                    infoIds.add(vidFilterId);
                    // endregion cache
                }

            }
            vid2AlarmEnd.remove(vidFilterId);
        } else if (ret == 2) {
            if (!CollectionUtils.isEmpty(list) && list.contains(filterId)) {
                String countTime = vid2AlarmEnd.get(vidFilterId);
                if (!StringUtils.isEmpty(countTime)) {

                    String[] ctArr = countTime.split("_");
                    vid2AlarmEnd.put(vidFilterId, Integer.valueOf(ctArr[0]) + 1 + "_" + ctArr[1]);
                    if (Integer.valueOf(ctArr[0]) == (alarmNum - 1)) {
                        //上条报警，本条不报警，说明是【结束报警】，发送结束报警报文
                        String alarmId = alarmMap.get(vid + "#" + filterId);

                        Map<String, Object> sendMsg = new TreeMap<>();
                        sendMsg.put(DataKey.VEHICLE_ID, vid);
                        sendMsg.put("ALARM_ID", alarmId);
                        sendMsg.put("STATUS", 3);
                        sendMsg.put("TIME", ctArr[1]);
                        sendMsg.put("CONST_ID", filterId);
                        String alarmEnd = JSON_UTILS.toJson(sendMsg);

                        sendMsg.put("ALARM_NAME", alarmName);
                        sendMsg.put("ALARM_LEVEL", alarmLevel);
                        sendMsg.put("LEFT1", left1);
                        sendMsg.put("LEFT2", left2);
                        sendMsg.put("RIGHT1", right1);
                        sendMsg.put("RIGHT2", right2);
                        String alarmHbase = JSON_UTILS.toJson(sendMsg);

                        //kafka存储
                        sendAlarmKafka(SysDefine.VEH_ALARM, vehAlarmTopic, vid, alarmEnd);
                        //hbase存储
                        sendAlarmKafka(SysDefine.VEH_ALARM_REALINFO_STORE, vehAlarmStoreTopic, vid, alarmHbase);
                        //redis存储
                        saveRedis(vid, "0", ctArr[1]);
                        alarmMap.remove(vid + "#" + filterId);
                        filterMap.get(vid).remove(filterId);
                        vid2AlarmEnd.remove(vidFilterId);
                    }
                } else {
                    vid2AlarmEnd.put(vidFilterId, "1_" + time);
                }
            }
            vid2AlarmInfo.remove(vidFilterId);
        }
    }

    /**
     * 存储更新redis
     */
    private void saveRedis(String vid, String status, String time) {
        vid2Alarm.put(vid, status + "_" + System.currentTimeMillis() + "_" + time);
    }

    private synchronized void sendAlarmKafka(
        @NotNull final String streamId,
        @NotNull final String topic,
        @NotNull final String vid,
        @NotNull final String message) {

        collector.emit(streamId, new Values(topic, vid, message));
    }

    private synchronized void sendToNext(
        @NotNull final String vid,
        Object message) {

        collector.emit(SysDefine.SYNES_GROUP, new Values(vid, message));
    }

    private int diffMarkValid(double value, int mark, double right1, double right2) {
        int ret = 0;
        switch (mark) {
            case 1:
                if (value == right1) {
                    ret = 1;
                } else {
                    ret = 2;
                }
                break;
            case 2:
                if (value < right1) {
                    ret = 1;
                } else {
                    ret = 2;
                }
                break;
            case 3:
                if (value <= right1) {
                    ret = 1;
                } else {
                    ret = 2;
                }
                break;
            case 4:
                if (value > right1) {
                    ret = 1;
                } else {
                    ret = 2;
                }
                break;
            case 5:
                if (value >= right1) {
                    ret = 1;
                } else {
                    ret = 2;
                }
                break;
            case 6:
                if (value > right1 && value < right2) {
                    ret = 1;
                } else {
                    ret = 2;
                }
                break;
            case 7:
                if (value >= right1 && value < right2) {
                    ret = 1;
                } else {
                    ret = 2;
                }
                break;
            case 8:
                if (value > right1 && value <= right2) {
                    ret = 1;
                } else {
                    ret = 2;
                }
                break;
            case 9:
                if (value >= right1 && value <= right2) {
                    ret = 1;
                } else {
                    ret = 2;
                }
                break;
            default:
                break;
        }

        return ret;

    }

    private double diffMarkValid2(int mark, double left1, double left2) {
        double ret = 0;
        switch (mark) {
            case 1:
                ret = left1 + left2;
                break;
            case 2:
                ret = left1 - left2;
                break;
            case 3:
                ret = left1 * left2;
                break;
            case 4:
                if (0 == left2) {
                    break;
                }

                ret = left1 / left2;
                break;
            default:
                break;
        }

        return ret;

    }

    private void removeByVid(String vid) {
        try {
            if (null != vid) {
                Set<String> idSet = vidAlarmIds.get(vid);
                if (null != idSet && idSet.size() > 0) {
                    for (String id : idSet) {
                        vid2AlarmInfo.remove(id);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void timeOutOver(long offtime) {
        try {

            if (needListenAlarms.size() > 0) {
                List<String> needReListens = new LinkedList<>();

                String vid = needListenAlarms.poll();
                while (null != vid) {
                    needListenAlarmSet.remove(vid);
                    if (null != lastCache && lastCache.size() > 0
                        && null != filterMap && filterMap.size() > 0) {
                        long now = System.currentTimeMillis();

                        if (filterMap.containsKey(vid)) {

                            Map<String, String> dat = lastCache.get(vid);
                            if (null != dat && dat.size() > 0) {
                                if (dat.containsKey(SysDefine.ONLINE_UTC)) {

                                    long timels = Long.parseLong(dat.get(SysDefine.ONLINE_UTC));
                                    if (now - timels > offtime) {
                                        String vType = dat.get("VTYPE");
                                        if (!StringUtils.isEmpty(vid)
                                            && !StringUtils.isEmpty(vType)) {

                                            List<EarlyWarn> warns = EarlyWarnsGetter.allWarnArrsByType(vType);
                                            if (!CollectionUtils.isEmpty(warns)) {

                                                sendOverAlarmMessage(vid);
                                            }
                                        }

                                    } else {
                                        needReListens.add(vid);
                                    }

                                }

                            }
                        }

                    }

                    vid = needListenAlarms.poll();
                }

                if (needReListens.size() > 0) {
                    for (String key : needReListens) {
                        if (!needListenAlarmSet.contains(key)) {
                            needListenAlarmSet.add(key);
                            needListenAlarms.offer(key);
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}