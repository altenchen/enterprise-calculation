package storm.bolt.deal.norm;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import storm.dto.alarm.CoefficientOffset;
import storm.dto.alarm.CoefficientOffsetGetter;
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

    /**
     * <vid, [rule]>, 车辆告警中的规则
     */
    private Map<String, List<String>> ruleMap = Maps.newHashMap();

    /**
     * 连续多少条报警才发送通知
     */
    private static int alarmContinueMaxCount = 10;

    /**
     * 告警消息 kafka 输出 topic
     */
    private String vehAlarmTopic;

    /**
     * HBase 车辆报警状态存储 kafka 输出 topic
     */
    private String vehAlarmStoreTopic;

    /**
     * <vid#ruleId, vid_time_ruleId>
     */
    private Map<String, String> alarmMap = Maps.newHashMap();

    /**
     * 车辆最大的数据帧采集时间
     */
    private Map<String, String> vehDataMap = Maps.newHashMap();

    /**
     * 车辆正在报警信息缓存<vid, 是否报警_系统处理时间_最后报警时间>
     */
    private Map<String, String> vid2Alarm = Maps.newHashMap();

    /**
     * 车辆结束报警信息缓存 <vid_ruleId, >
     */
    private Map<String, String> vid2AlarmEnd = Maps.newHashMap();

    /**
     * 车辆开始报警信息缓存, <vid_ruleId, 累计帧数_0_告警开始时间>
     */
    private Map<String, String> vid2AlarmInfo = Maps.newHashMap();

    /**
     * <vid, [vid_ruleId]>, 车辆已触发正在报警的规则集合.
     */
    private Map<String, Set<String>> vidAlarmIds = Maps.newHashMap();

    /**
     * 车辆的最后一帧数据
     */
    private Map<String, Map<String, String>> lastCache = Maps.newHashMap();

    /**
     * 默认300秒同步数据库新建规则
     */
    private long flushtime = 300;

    /**
     * 离线超时时间
     */
    private static long onlineTimeout = 600 * 1000;

    /**
     * 默认预留车辆数, 100万辆
     */
    private final int buffsize = 5000000;

    /**
     * 需要监听的车辆 <vid>
     */
    private final Queue<String> needListenAlarms = new LinkedBlockingQueue<>(buffsize);

    /**
     * needListenAlarms 防重 <vid>
     */
    private final Set<String> needListenAlarmSet = new HashSet<>(buffsize / 5);

    @SuppressWarnings("AlibabaMethodTooLong")
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
                alarmContinueMaxCount = NumberUtils.toInt(str, 0);
            }

            flushtime = Long.parseLong(stormConf.get(SysDefine.DB_CACHE_FLUSH_TIME_SECOND).toString());

            Object offli = stormConf.get(StormConfigKey.REDIS_OFFLINE_SECOND);
            if (null != offli) {
                onlineTimeout = 1000 * Long.valueOf(offli.toString());
            }
        } catch (Exception e) {
            LOG.warn("初始化配置异常", e);
        }

        try {

            /**
             * 定时重新初始化预警规则和偏移系数规则.
             */
            class RebulidClass implements Runnable {

                @Override
                public void run() {
                    try {
                        EarlyWarnsGetter.rebuild();
                        CoefficientOffsetGetter.rebuild();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }

            // TODO: 从专用 Spout 发射过来
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new RebulidClass(), 0, flushtime, TimeUnit.SECONDS);

            /**
             * 如果车辆下线, 则发送预警, 否则将车辆重新加入监听队列
             */
            class TimeOutClass implements Runnable {

                @Override
                public void run() {
                    try {
                        timeOutOver();
                    } catch (Exception e) {
                        LOG.warn("TimeOutClass:", e);
                    }
                }

            }
            // TODO: 使用storm心跳帧来取代
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new TimeOutClass(), 0, flushtime, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("启动定时任务异常", e);
        }
    }

    @SuppressWarnings("AlibabaMethodTooLong")
    @Override
    public void execute(@NotNull final Tuple input) {
        if (input.getSourceStreamId().equals(SysDefine.SPLIT_GROUP)) {
            final String vid = input.getString(0);
            final Map<String, String> data = (Map<String, String>) input.getValue(1);

            if (!data.containsKey(DataKey.TIME)
                || StringUtils.isEmpty(data.get(DataKey.TIME))) {
                return;
            }
            final String messageType = data.get(DataKey.MESSAGE_TYPE);

            final String linkType = data.get(SUBMIT_LINKSTATUS.LINK_TYPE);
            if (
                // 实时信息上报
                CommandType.SUBMIT_REALTIME.equals(messageType)
                    || (
                    // 链接状态通知
                    CommandType.SUBMIT_LINKSTATUS.equals(messageType)
                        // 是否离线通知
                        && SUBMIT_LINKSTATUS.isOfflineNotice(linkType)
                    )
                    || (
                    // 终端注册消息
                    CommandType.SUBMIT_LOGIN.equals(messageType)
                        && (
                        // 登出流水号
                        data.containsKey(SUBMIT_LOGIN.LOGOUT_SEQ)
                            // 登出时间
                            || data.containsKey(SUBMIT_LOGIN.LOGOUT_TIME))
                    )
            ) {
                try {
                    // 这里才是干事的入口, 下面只是刷缓存, 不过这个未知似乎少了些状态...
                    processAlarm(data, messageType);
                } catch (Exception e) {
                    LOG.warn("软报警分析出错! [{}]", data);
                }
            }

            // 以下代码计算一些状态之后存更新缓存
            if (
                // 实时信息上报
                CommandType.SUBMIT_REALTIME.equals(messageType)
                    // 终端注册消息
                    || CommandType.SUBMIT_LOGIN.equals(messageType)
                    // 状态信息上报
                    || CommandType.SUBMIT_TERMSTATUS.equals(messageType)
                    // 车辆运行状态
                    || CommandType.SUBMIT_CARSTATUS.equals(messageType)) {

                // 更新告警状态和
                try {

                    // 车辆报警信息缓存(vid----是否报警_最后报警时间)
                    final String string = vid2Alarm.get(vid);
                    if (!StringUtils.isEmpty(string)) {
                        final String[] alarmStr = string.split("_", 3);
                        data.put(SysDefine.IS_ALARM, alarmStr[0]);
                        data.put(SysDefine.ALARMUTC, alarmStr[1]);
                    }

                } catch (Exception e) {
                    LOG.warn("实时数据redis存储出错! [{}]", data);
                }
            }
            // 车辆链接状态帧, 更新上下线状态和告警状态
            else if (CommandType.SUBMIT_LINKSTATUS.equals(messageType)) {

                final Map<String, String> linkStatusData = Maps.newTreeMap();

                // 上线
                if (SUBMIT_LINKSTATUS.isOnlineNotice(linkType)) {
                    linkStatusData.put(DataKey._10002_IS_ONLINE, "1");
                }
                // 离线
                else if (SUBMIT_LINKSTATUS.isOfflineNotice(linkType)) {
                    linkStatusData.put(DataKey._10002_IS_ONLINE, "0");
                    linkStatusData.put(SysDefine.IS_ALARM, "0");
                }

                data.putAll(linkStatusData);
            }

            if (
                // 实时信息上报
                CommandType.SUBMIT_REALTIME.equals(messageType)
                    // 终端注册消息
                    || CommandType.SUBMIT_LOGIN.equals(messageType)
                    // 链接状态通知
                    || CommandType.SUBMIT_LINKSTATUS.equals(messageType)
                    // 状态信息上报
                    || CommandType.SUBMIT_TERMSTATUS.equals(messageType)
                    // 车辆运行状态
                    || CommandType.SUBMIT_CARSTATUS.equals(messageType)) {

                // 更新缓存
                lastCache.compute(vid, (key, oldValue) -> {
                    final Map<String, String> newValue = null == oldValue ? Maps.newConcurrentMap() : oldValue;
                    data.forEach((k, v) -> {
                        if (StringUtils.isNotBlank(v)) {
                            newValue.put(k, v);
                        }
                    });
                    return newValue;
                });
            }

            if (CommandType.SUBMIT_REALTIME.equals(messageType)) {
                try {
                    final String collectTime = data.get(DataKey._2000_COLLECT_TIME);
                    if (!StringUtils.isEmpty(collectTime)) {
                        final String lastCollectTime = vehDataMap.get(vid);
                        if (null == lastCollectTime || (lastCollectTime.compareTo(collectTime) < 0)) {
                            vehDataMap.put(vid, collectTime);
                        }
                    }
                } catch (final Exception e) {
                    LOG.warn("更新最大车辆实时数据采集时间异常", e);
                }
            }

        } else {
            LOG.warn("未知的流[{}][{}]", input.getSourceComponent(), input.getSourceStreamId());
        }
    }

    @Override
    public void declareOutputFields(@NotNull final OutputFieldsDeclarer declarer) {

        KafkaStream.declareOutputFields(declarer, SysDefine.VEH_ALARM);
        KafkaStream.declareOutputFields(declarer, SysDefine.VEH_ALARM_REALINFO_STORE);

        // 发往 FaultBolt
        declarer.declareStream(SysDefine.FAULT_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * 软报警处理
     */
    private void processAlarm(@NotNull final Map<String, String> data, @NotNull final String messageType) {
        if (MapUtils.isEmpty(data)) {
            return;
        }

        final String vid = data.get(DataKey.VEHICLE_ID);
        final String vehType = data.get(DataKey.VEHICLE_TYPE);
        if (StringUtils.isEmpty(vid)
            || StringUtils.isEmpty(vehType)) {
            return;
        }

        if (
            // 链接状态通知
            CommandType.SUBMIT_LINKSTATUS.equals(messageType)
            // 终端注册消息
            || CommandType.SUBMIT_LOGIN.equals(messageType)) {
            try {
                sendOverAlarmMessage(vid);
            } catch (Exception e) {
                LOG.warn("自动发送结束报警异常", e);
            }
            return;
        }

        final List<EarlyWarn> warns = EarlyWarnsGetter.allWarnArrsByType(vehType);
        if (CollectionUtils.isEmpty(warns)) {
            return;
        }

        try {
            for (EarlyWarn warn : warns) {
                if (null == warn) {
                    continue;
                }

                int result;
                // 没有依赖项, 直接处理
                if (null == warn.dependId) {

                    result = processSingleAlarm(vid, warn, data);
                    sendAlarmMessage(result, vid, warn, data);

                }
                // TODO: 有依赖项, 如果处理结果为1, 再处理父及约束? 是不是反了?!!
                else {
                    final EarlyWarn warndepend = EarlyWarnsGetter.getEarlyByDependId(warn.dependId);
                    result = processSingleAlarm(vid, warn, data);
                    if (result == 1) {

                        //先判断父级约束是否成立，如果成立则继续判断子级约束

                        if (null != warndepend) {
                            result = processSingleAlarm(vid, warndepend, data);
                        }
                    }
                    sendAlarmMessage(result, vid, warndepend, data);
                }
            }
        } catch (Exception e) {
            LOG.warn("按预警规则处理时异常", e);
        }
    }

    /**
     * 车辆发离线通知，系统自动发送结束报警通知
     *
     * @param vid
     */
    private void sendOverAlarmMessage(@NotNull final String vid) {
        if (ruleMap.containsKey(vid)) {
            String time = vid2Alarm.get(vid).split("_")[2];
            List<String> list = ruleMap.get(vid);
            for (String filterId : list) {
                //上条报警，本条不报警，说明是结束报警，发送结束报警报文
                String alarmId = alarmMap.get(vid + "#" + filterId);
                EarlyWarn warn = EarlyWarnsGetter.getEarlyByDependId(filterId);
                if (null == warn) {
                    continue;
                }

                String alarmName = warn.ruleName;
                int alarmLevel = warn.levels;
                String left1 = warn.left1DataKey;

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
                saveToRedis(vid, "0", time);
                alarmMap.remove(vid + "#" + filterId);
            }
            ruleMap.remove(vid);
        }
        //离线重置所有报警约束
        removeByVid(vid);
    }

    /**
     * 处理单个预警规则
     * @param vid 车辆Id
     * @param warn 预警规则
     * @param dataMap 数据集
     * @return
     */
    private int processSingleAlarm(String vid, EarlyWarn warn, Map<String, String> dataMap) {
        int ret = 0;
        try {
            if (null != warn) {
                //左1数据项ID
                String left1 = warn.left1DataKey;
                //偏移系数，
                final CoefficientOffset left1CoefficientOffset = CoefficientOffsetGetter.getCoefficientOffset(left1);
                String left1Value = dataMap.get(left1);
                //上传的实时数据包含左1字段 才进行预警判定
                if (StringUtils.isEmpty(left1Value)) {
                    return ret;
                }
                boolean stringIsNum = org.apache.commons.lang.math.NumberUtils.isNumber(left1Value);

                if (null != left1CoefficientOffset
                    && left1CoefficientOffset.isNumber()
                    && !stringIsNum
                ) {
                    return ret;
                }
                if (null == left1CoefficientOffset
                    && !stringIsNum) {
                    return ret;
                }

                int leftExp = Integer.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(warn.leftExpression) ? warn.leftExpression : "0");
                //左2数据项ID
                String left2 = warn.left2DataKey;
                int midExp = Integer.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(warn.middleExpression) ? warn.middleExpression : "0");
                double right1 = warn.right1Value;
                double right2 = warn.right2Value;
                //左二字段为空，L2_ID为空  根据EXPR_MID，和R1_VAL, R2_VAL判断
                if (StringUtils.isEmpty(left2)) {

                    //不需要处理偏移和系数
                    if (null == left1CoefficientOffset) {
                        double left1_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(left1Value) ? left1Value : "0");
                        //判断是否软报警条件(true/false)
                        ret = diffMarkValid(left1_value, midExp, right1, right2);
                    } else if (left1CoefficientOffset.isNumber()) {
                        double left1_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(left1Value) ? left1Value : "0");
                        left1_value = (left1_value - left1CoefficientOffset.offset) / left1CoefficientOffset.coefficient;
                        //判断是否软报警条件(true/false)
                        ret = diffMarkValid(left1_value, midExp, right1, right2);
                    } else if (left1CoefficientOffset.isArray()) {
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
                                            value = (value - left1CoefficientOffset.offset) / left1CoefficientOffset.coefficient;
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
                        if (null != left1CoefficientOffset && left1CoefficientOffset.isArray()) {
                            return ret;
                        }

                        final CoefficientOffset left2CoefficientOffset = CoefficientOffsetGetter.getCoefficientOffset(left2);
                        if (null != left2CoefficientOffset && left2CoefficientOffset.isArray()) {
                            return ret;
                        }

                        if (!org.apache.commons.lang.math.NumberUtils.isNumber(left1Value)
                            || !org.apache.commons.lang.math.NumberUtils.isNumber(left2Value)) {
                            return ret;
                        }

                        double left1_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(left1Value) ? left1Value : "0");
                        if (null != left1CoefficientOffset) {
                            left1_value = (left1_value - left1CoefficientOffset.offset) / left1CoefficientOffset.coefficient;
                        }

                        double left2_value = Double.valueOf(org.apache.commons.lang.math.NumberUtils.isNumber(left2Value) ? left2Value : "0");
                        if (null != left2CoefficientOffset) {
                            left2_value = (left2_value - left2CoefficientOffset.offset) / left2CoefficientOffset.coefficient;
                        }

                        double left_value = diffMarkValid2(leftExp, left1_value, left2_value);

                        //判断是否软报警条件(true/false)
                        ret = diffMarkValid(left_value, midExp, right1, right2);
                        return ret;

                    } else {
                        String lastData = "";
                        final Map<String, String> last = lastCache.get(vid);
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
                                                        if (null != left1CoefficientOffset) {
                                                            left1_value = (left1_value - left1CoefficientOffset.offset) / left1CoefficientOffset.coefficient;
                                                            left2_value = (left2_value - left1CoefficientOffset.offset) / left1CoefficientOffset.coefficient;
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
                            if (null != left1CoefficientOffset) {
                                left1_value = (left1_value - left1CoefficientOffset.offset) / left1CoefficientOffset.coefficient;
                                left2_value = (left2_value - left1CoefficientOffset.offset) / left1CoefficientOffset.coefficient;
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

    /**
     *
     * @param result 预警处理结果, 1-告警开始
     * @param vid 车辆Id
     * @param warn 预警规则
     * @param data 数据集
     */
    private void sendAlarmMessage(
        final int result,
        final String vid,
        final EarlyWarn warn,
        final Map<String, String> data) {

        if (null == warn) {
            return;
        }

        // 最高报警等级
        final int alarmGb = NumberUtils.toInt(data.get(DataKey._2920_ALARM_STATUS), 0);

        final String ruleId = warn.ruleId;
        final String alarmName = warn.ruleName;
        final int alarmLevel = Math.max(warn.levels, alarmGb);

        // 左 1 数据项 ID
        final String left1 = warn.left1DataKey;
        // 左 2 数据项 ID
        final String left2 = warn.left2DataKey;
        // 右 1 数据值
        final double right1 = warn.right1Value;
        // 右 2 数据值
        final double right2 = warn.right2Value;

        final String time = data.get(DataKey.TIME);

        long alarmUtc = 0;
        try {
            alarmUtc = DateUtils.parseDate(time, new String[]{FormatConstant.DATE_FORMAT}).getTime();
        } catch (ParseException e) {
            LOG.warn("反序列化时间异常", e);
        }
        if (0 == alarmUtc) {
            alarmUtc = System.currentTimeMillis();
        }

        final String vidRuleId = vid + "_" + ruleId;

        // 车辆告警中的规则
        final List<String> list = ruleMap.get(vid);

        if (result == 1) {
            //报警缓存包含vid，且vid对应的list含有此约束id，也就是此类型的报警，就说明上一条已报警

            if (!CollectionUtils.isEmpty(list) && list.contains(ruleId)) {
                //上条报警，本条也报警，说明是【报警进行中】，发送报警进行中报文, 貌似这里啥也没干.

                //redis存储
                saveToRedis(vid, "1", time);
                vid2AlarmInfo.put(vidRuleId, (alarmContinueMaxCount + 1) + "_0_" + alarmUtc);

                // region cache
                final Set<String> infoIds = vidAlarmIds.computeIfAbsent(vid, k -> Sets.newHashSet());
                infoIds.add(vidRuleId);
                // endregion cache

            } else {
                //上条不报警，本条报警，说明是【开始报警】，发送开始报警报文

                // 格式化报警开始状态, 报警开始连续帧数_0_报警通知发出时间
                final String formatString = vid2AlarmInfo.get(vidRuleId);

                if (!StringUtils.isEmpty(formatString)) {
                    // 有格式化报警状态, 说明是报警开始续帧

                    String[] infoArr = formatString.split("_", 3);
                    if (infoArr.length >= 3) {

                        // 报警开始连续帧数
                        final int alarmContinueCount = Integer.valueOf(infoArr[0]);
                        // 固定 "0"
                        final long alarmTime = Long.parseLong(infoArr[1]);
                        // 报警通知发出时间
                        final long lastAlarmUtc = Long.parseLong(infoArr[2]);

                        vid2AlarmInfo.put(vidRuleId, (alarmContinueCount + 1) + "_" + alarmTime + "_" + lastAlarmUtc);

                        // region cache

                        // 标记当前车辆正在报警中
                        final Set<String> infoIds = vidAlarmIds.computeIfAbsent(vid, k -> new HashSet<>());
                        infoIds.add(vidRuleId);

                        // endregion cache

                        // 根据数据项的预警配置进行判断，条件成立针对最后预警发生时间或者个数累计进行进行判定，若超过3分钟或连续累计超过10次的条件方认为成立
                        if (alarmContinueCount + 1 >= alarmContinueMaxCount) {
                            // 累计达到 alarmContinueMaxCount 指定的连续帧数条件

                            final String alarmId = vid + "_" + time + "_" + ruleId;
                            alarmMap.put(vid + "#" + ruleId, alarmId);

                            // 车辆告警中的规则
                            final List<String> alarmList = ruleMap.computeIfAbsent(vid, k -> new LinkedList<>());
                            alarmList.add(ruleId);
                            ruleMap.put(vid, alarmList);

                            if (!needListenAlarmSet.contains(vid)) {
                                needListenAlarmSet.add(vid);
                                needListenAlarms.offer(vid);
                            }

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
                            sendMsg.put("CONST_ID", ruleId);
                            sendMsg.put("ALARM_LEVEL", alarmLevel);

                            final String alarmStart = JSON_UTILS.toJson(sendMsg);
                            //发送kafka提供数据库存储
                            sendAlarmKafka(SysDefine.VEH_ALARM, vehAlarmTopic, vid, alarmStart);

                            sendMsg.put("ALARM_NAME", alarmName);
                            sendMsg.put("LEFT1", left1);
                            sendMsg.put("LEFT2", left2);
                            sendMsg.put("RIGHT1", right1);
                            sendMsg.put("RIGHT2", right2);

                            final String alarmHbase = JSON_UTILS.toJson(sendMsg);
                            //hbase存储
                            sendAlarmKafka(SysDefine.VEH_ALARM_REALINFO_STORE, vehAlarmStoreTopic, vid, alarmHbase);

                            sendMsg.put("UTC_TIME", lastAlarmUtc);

                            //redis存储
                            saveToRedis(vid, "1", lastAlarmUtcString);

                            // 更新格式化信息中的告警时间(最后一项)
                            vid2AlarmInfo.put(vidRuleId, (alarmContinueCount + 1) + "_" + alarmTime + "_" + alarmUtc);
                        }
                    }

                } else {
                    // 无格式化报警状态, 说明是报警开始首帧

                    vid2AlarmInfo.put(vidRuleId, "1_0_" + alarmUtc);

                    // region cache

                    // 标记当前车辆正在报警中
                    final Set<String> infoIds = vidAlarmIds.computeIfAbsent(vid, k -> new HashSet<>());
                    infoIds.add(vidRuleId);

                    // endregion cache
                }

            }

            // 取消标记当前车辆报警结束
            vid2AlarmEnd.remove(vidRuleId);

        } else if (result == 2) {

            if (!CollectionUtils.isEmpty(list) && list.contains(ruleId)) {
                // 当前车辆存在当前告警

                // 格式化报警结束状态, 报警结束连续帧数_报警结束首帧时间
                final String formatString = vid2AlarmEnd.get(vidRuleId);

                if (!StringUtils.isEmpty(formatString)) {
                    // 有格式化报警状态, 说明是报警结束续帧

                    final String[] ctArr = formatString.split("_");
                    vid2AlarmEnd.put(vidRuleId, Integer.valueOf(ctArr[0]) + 1 + "_" + ctArr[1]);

                    if (Integer.valueOf(ctArr[0]) == (alarmContinueMaxCount - 1)) {
                        //上条报警，本条不报警，说明是【结束报警】，发送结束报警报文
                        final String alarmId = alarmMap.get(vid + "#" + ruleId);

                        Map<String, Object> sendMsg = new TreeMap<>();
                        sendMsg.put(DataKey.VEHICLE_ID, vid);
                        sendMsg.put("ALARM_ID", alarmId);
                        sendMsg.put("STATUS", 3);
                        sendMsg.put("TIME", ctArr[1]);
                        sendMsg.put("CONST_ID", ruleId);

                        final String alarmEnd = JSON_UTILS.toJson(sendMsg);
                        //kafka存储
                        sendAlarmKafka(SysDefine.VEH_ALARM, vehAlarmTopic, vid, alarmEnd);

                        sendMsg.put("ALARM_NAME", alarmName);
                        sendMsg.put("ALARM_LEVEL", alarmLevel);
                        sendMsg.put("LEFT1", left1);
                        sendMsg.put("LEFT2", left2);
                        sendMsg.put("RIGHT1", right1);
                        sendMsg.put("RIGHT2", right2);

                        final String alarmHbase = JSON_UTILS.toJson(sendMsg);
                        //hbase存储
                        sendAlarmKafka(SysDefine.VEH_ALARM_REALINFO_STORE, vehAlarmStoreTopic, vid, alarmHbase);

                        //redis存储
                        saveToRedis(vid, "0", ctArr[1]);

                        // 从报警中的车辆规则里移除
                        alarmMap.remove(vid + "#" + ruleId);
                        // 移除车辆告警中的规则
                        ruleMap.get(vid).remove(ruleId);
                        // 移除车辆结束报警信息缓存
                        vid2AlarmEnd.remove(vidRuleId);
                    }
                } else {
                    // 无格式化报警状态, 说明是报警结束首帧

                    vid2AlarmEnd.put(vidRuleId, "1_" + time);
                }
            }

            // 清除格式化报警状态
            vid2AlarmInfo.remove(vidRuleId);
        }
    }

    /**
     * 存储到 vid2Alarm
     */
    private void saveToRedis(
        @NotNull final String vid,
        @NotNull final String status,
        @NotNull final String time) {

        // TODO: 同步到 redis
        vid2Alarm.put(vid, status + "_" + System.currentTimeMillis() + "_" + time);
    }

    private synchronized void sendAlarmKafka(
        @NotNull final String streamId,
        @NotNull final String topic,
        @NotNull final String vid,
        @NotNull final String message) {

        collector.emit(streamId, new Values(topic, vid, message));
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

    private void timeOutOver() {
        try {

            if (needListenAlarms.size() > 0) {
                final List<String> needReListens = new LinkedList<>();

                String vid = needListenAlarms.poll();
                while (null != vid) {
                    needListenAlarmSet.remove(vid);
                    if (MapUtils.isNotEmpty(lastCache)
                        && MapUtils.isNotEmpty(ruleMap)) {
                        final long currentTimeMillis = System.currentTimeMillis();

                        if (ruleMap.containsKey(vid)) {

                            final Map<String, String> data = lastCache.get(vid);
                            if (MapUtils.isNotEmpty(data)) {
                                if (data.containsKey(SysDefine.ONLINE_UTC)) {

                                    final long onlineUtc = Long.parseLong(data.get(SysDefine.ONLINE_UTC));

                                    // 如果车辆下线, 则发送预警, 否则将车辆重新加入监听队列
                                    if (currentTimeMillis - onlineUtc > onlineTimeout) {
                                        final String vehicleType = data.get(DataKey.VEHICLE_TYPE);
                                        if (StringUtils.isNotEmpty(vid)
                                            && StringUtils.isNotEmpty(vehicleType)) {

                                            final List<EarlyWarn> warns = EarlyWarnsGetter.allWarnArrsByType(vehicleType);
                                            if (!CollectionUtils.isEmpty(warns)) {
                                                // 车辆发离线通知，系统自动发送结束报警通知
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

                // 重新加入监听队列
                if (CollectionUtils.isNotEmpty(needReListens)) {
                    for (String key : needReListens) {
                        if (!needListenAlarmSet.contains(key)) {
                            needListenAlarmSet.add(key);
                            needListenAlarms.offer(key);
                        }
                    }
                }
            }

        } catch (Exception e) {
            LOG.warn("下线车辆告警处理异常.", e);
        }
    }
}