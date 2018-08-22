package storm.bolt.deal.norm;

import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Nullable;
import storm.constant.StreamFieldKey;
import storm.handler.cusmade.TimeOutOfRangeNotice;
import org.apache.commons.lang.StringUtils;
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
import storm.kafka.bolt.KafkaBoltTopic;
import storm.protocol.*;
import storm.stream.FromFilterToCarNoticeStream;
import storm.stream.KafkaStream;
import storm.system.DataKey;
import storm.system.ProtocolItem;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.JsonUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 将报文转换成字典, 最后通过tuple发出
 * @author xzp
 */
public class FilterBolt extends BaseRichBolt {

    // region 类常量

    private static final long serialVersionUID = 1700001L;

    private static final Logger LOG = LoggerFactory.getLogger(FilterBolt.class);

    private static final ConfigUtils CONFIG_UTILS = ConfigUtils.getInstance();

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final FromFilterToCarNoticeStream TO_CAR_NOTICE_STREAM = FromFilterToCarNoticeStream.getInstance();

    private static final Pattern MSG_REGEX = Pattern.compile("^([^ ]+) (\\d+) ([^ ]+) ([^ ]+) \\{VID:([^,]*)(?:,([^}]+))*\\}$");

    /**
     * 是否启用时间异常通知
     */
    private static final boolean ENABLE_TIME_OUT_OF_RANGE_NOTICE;

    static {

        final Properties sysDefine = CONFIG_UTILS.sysDefine;

        {
            final String noticeTimeEnableString = sysDefine.getProperty(SysDefine.NOTICE_TIME_ENABLE);
            final boolean noticeTimeEnable = BooleanUtils.toBoolean(noticeTimeEnableString);
            ENABLE_TIME_OUT_OF_RANGE_NOTICE = noticeTimeEnable;
            if (LOG.isInfoEnabled()) {
                LOG.info("时间异常通知已{}.", ENABLE_TIME_OUT_OF_RANGE_NOTICE ? "启用" : "禁用");
            }
        }

        {
            final String noticeTimeRangeAbsMillisecondString = sysDefine.getProperty(SysDefine.NOTICE_TIME_RANGE_ABS_MILLISECOND);
            final long noticeTimeRangeAbsMillisecond = NumberUtils.toLong(noticeTimeRangeAbsMillisecondString, TimeOutOfRangeNotice.DEFAULT_TIME_RANGE_MILLISECOND);
            TimeOutOfRangeNotice.setTimeRangeMillisecond(noticeTimeRangeAbsMillisecond);
        }
    }

    // endregion 类常量

    // region 对象常量

    private final TimeOutOfRangeNotice timeOutOfRangeNotice = new TimeOutOfRangeNotice();

    private final OnlineProcessor onlineProcessor = new OnlineProcessor();

    private final TimeProcessor timeProcessor = new TimeProcessor();

    private final ChargeProcessor chargeProcessor = new ChargeProcessor();

    private final PowerBatteryAlarmFlagProcessor powerBatteryAlarmFlagProcessor = new PowerBatteryAlarmFlagProcessor();

    private final AlarmProcessor alarmProcessor = new AlarmProcessor();

    private final StatusFlagsProcessor statusFlagsProcessor = new StatusFlagsProcessor();

    // endregion 对象常量

    // region 对象变量

    private OutputCollector collector;

    private FromFilterToCarNoticeStream.Emiter toCarNoticeStreamEmiter;

    // endregion 对象变量

    @Override
    public void prepare(@NotNull final Map stormConf, @NotNull final TopologyContext context, @NotNull final OutputCollector collector) {
        this.collector = collector;
        toCarNoticeStreamEmiter = TO_CAR_NOTICE_STREAM.buildStreamEmiter(collector);
    }

    @Override
    public void execute(@NotNull final Tuple input) {

        collector.ack(input);

        final String msg = input.getStringByField(StreamFieldKey.MSG);
        final Matcher matcher = MSG_REGEX.matcher(msg);
        if (!matcher.find()) {
            LOG.warn("无效的元组[{}]", input.toString());
            return;
        }

        final String prefix = matcher.group(1);
        final String serialNo = matcher.group(2);
        final String vin = matcher.group(3);
        final String cmd = matcher.group(4);
        final String vid = matcher.group(5);
        final String content = matcher.group(6);

        // 只处理主动发送数据
        if(!CommandType.SUBMIT.equals(prefix)) {
            return;
        }

        // TODO: 从黑名单模式改成白名单模式
        if (
            // 如果是补发数据直接忽略
            CommandType.SUBMIT_HISTORY.equals(cmd)
                // 过滤租赁点更新数据
                || CommandType.RENTALSTATION.equals(cmd)
                // 过滤充电站更新数据
                || CommandType.CHARGESTATION.equals(cmd)) {
            return;
        }

        final Map<String, String> data = Maps.newHashMapWithExpectedSize(300);
        data.put(DataKey.PREFIX, prefix);
        data.put(DataKey.SERIAL_NO, serialNo);
        data.put(DataKey.VEHICLE_NUMBER, vin);
        data.put(DataKey.MESSAGE_TYPE, cmd);
        data.put(DataKey.VEHICLE_ID, vid);
        parseData(data, content);

        if (CommandType.SUBMIT_REALTIME.equals(cmd)) {

            // 时间异常判断, 如果有时间异常, 则认为是无效帧
            final Map<String, String> notice = timeOutOfRangeNotice.process(data);
            if(MapUtils.isNotEmpty(notice)) {

                if(ENABLE_TIME_OUT_OF_RANGE_NOTICE) {
                    sendNotice(vid, notice);
                }

                return;
            }

            // 判断是否充电
            chargeProcessor.fillChargingStatus(data);

            // 北京地标: 动力蓄电池报警标志解析存储, 见表20
            if (NumberUtils.isDigits(data.get(DataKey._2801_POWER_BATTERY_ALARM_FLAG_2801))) {
                powerBatteryAlarmFlagProcessor.fillPowerBatteryAlarm(data);
            }

            // 中国国标: 通用报警标志值, 见表18
            if (NumberUtils.isDigits(data.get(DataKey._3801_ALARM_MARK))) {
                alarmProcessor.fillAlarm(data);
            }

            // 北京地标: 车载终端状态解析存储, 见表23
            if (NumberUtils.isDigits(data.get(DataKey._3110_STATUS_FLAGS))) {
                statusFlagsProcessor.fillStatusFlags(data);
            }
        }

        // 增加utc字段，插入系统时间
        data.put(SysDefine.ONLINE_UTC, String.valueOf(System.currentTimeMillis()));

        // 计算在线状态(10002)和平台注册通知类型(TYPE)
        onlineProcessor.fillIsOnline(cmd, data);

        // 计算时间(TIME)加入data
        timeProcessor.fillTime(cmd, data);

        emit(vid, cmd, data);
    }

    @Override
    public void declareOutputFields(@NotNull final OutputFieldsDeclarer declarer) {
        declarer.declareStream(SysDefine.SPLIT_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
        declarer.declareStream(SysDefine.FENCE_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
        declarer.declareStream(SysDefine.SYNES_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
        TO_CAR_NOTICE_STREAM.declareStream(declarer);

        KafkaStream.declareOutputFields(declarer, SysDefine.CUS_NOTICE);
    }

    private void emit(@NotNull final String vid, @NotNull final String cmd, @NotNull final Map<String, String> data) {

        if (CommandType.SUBMIT_LINKSTATUS.equals(cmd)
            || CommandType.SUBMIT_LOGIN.equals(cmd)
            || CommandType.SUBMIT_TERMSTATUS.equals(cmd)
            || CommandType.SUBMIT_CARSTATUS.equals(cmd)) {
            // consumer: ES数据同步处理
            collector.emit(SysDefine.SYNES_GROUP, new Values(vid, data));
        }

        if (CommandType.SUBMIT_REALTIME.equals(cmd)) {
            // consumer: 电子围栏告警处理
            collector.emit(SysDefine.FENCE_GROUP, new Values(vid, data));
        }

        if (CommandType.SUBMIT_REALTIME.equals(cmd)
            || CommandType.SUBMIT_LINKSTATUS.equals(cmd)
            || CommandType.SUBMIT_LOGIN.equals(cmd)
            || CommandType.SUBMIT_TERMSTATUS.equals(cmd)
            || CommandType.SUBMIT_CARSTATUS.equals(cmd)) {
            // consumer: 车辆通知处理
            toCarNoticeStreamEmiter.emit(vid, data);
        }

        if (CommandType.SUBMIT_REALTIME.equals(cmd)
            || CommandType.SUBMIT_LINKSTATUS.equals(cmd)
            || CommandType.SUBMIT_LOGIN.equals(cmd)) {
            // 预警处理
            collector.emit(SysDefine.SPLIT_GROUP, new Values(vid, data));
        }
    }

    void sendNotice(
        @NotNull final String vid,
        @Nullable final Map<String, String> notice) {

        if(MapUtils.isEmpty(notice)){
            return;
        }

        final String json = JSON_UTILS.toJson(notice);
        sendToKafka(SysDefine.CUS_NOTICE, KafkaBoltTopic.NOTICE_TOPIC, vid, json);
    }

    void sendToKafka(
        @NotNull final String define,
        @NotNull final String topic,
        @NotNull final String vid,
        @Nullable final String message) {

        collector.emit(define, new Values(topic, vid, message));
    }

    @NotNull
    private void parseData(final Map<String, String> data, @NotNull final String dataString) {

        // 逗号
        int commaIndex = -1;
        do {
            // 冒号
            int colonIndex = dataString.indexOf((int) ':', commaIndex + 1);
            if(colonIndex == -1) {
                break;
            }

            final String key = dataString.substring(commaIndex + 1, colonIndex);

            commaIndex = dataString.indexOf((int)',', colonIndex + 1);
            if(commaIndex != -1) {
                final String value = dataString.substring(colonIndex + 1, commaIndex);
                data.put(key, value);
            } else {
                final String value = dataString.substring(colonIndex + 1);
                data.put(key, value);
                break;
            }
        } while (true);
    }

    private static class OnlineProcessor implements Serializable {

        private static final long serialVersionUID = 52118788960816896L;

        /**
         * 计算在线状态(10002)和平台注册通知类型(TYPE)
         * @param cmd
         * @param data
         */
        public void fillIsOnline(@NotNull final String cmd, @NotNull final Map<String, String> data) {

            if (
                // 实时数据
                !CommandType.SUBMIT_REALTIME.equals(cmd)
                    // 终端注册
                    && !CommandType.SUBMIT_LOGIN.equals(cmd)
                    // 状态信息上报
                    && !CommandType.SUBMIT_TERMSTATUS.equals(cmd)
                    // 车辆运行状态
                    && !CommandType.SUBMIT_CARSTATUS.equals(cmd)) {
                return;
            }

            // 设置在线状态(10002)为"1"
            data.put(DataKey._10002_IS_ONLINE, "1");

            // 如果不是终端注册报文
            if (!CommandType.SUBMIT_LOGIN.equals(cmd)) {
                return;
            }

            // 如果data包含登出流水号或者登出时间, 则设置在线状态(10002)为"0"
            // 并且将`平台注册通知类型`设置为`车机离线`
            // 否则将`平台注册通知类型`设置为`车机终端上线`
            if (data.containsKey(SUBMIT_LOGIN.LOGOUT_SEQ)
                || data.containsKey(SUBMIT_LOGIN.LOGOUT_TIME)) {

                data.put(DataKey._10002_IS_ONLINE, "0");
                data.put(ProtocolItem.REG_TYPE, "2");
            } else {
                data.put(ProtocolItem.REG_TYPE, "1");
            }
        }
    }

    private static class TimeProcessor implements Serializable {

        private static final long serialVersionUID = -1001473285507342121L;

        /**
         * 计算时间(TIME)加入data
         * @param cmd
         * @param data
         */
        public void fillTime(@NotNull final String cmd, @NotNull final Map<String, String> data) {

            if (CommandType.SUBMIT_REALTIME.equals(cmd)) {
                // 如果是实时数据, 则将TIME设置为数据采集时间
                data.put(DataKey.TIME, data.get(DataKey._2000_COLLECT_TIME));
            } else if (CommandType.SUBMIT_LOGIN.equals(cmd)) {
                // 如果是注册报文, 则将TIME设置为登入时间或者登出时间或者注册时间
                if (data.containsKey(SUBMIT_LOGIN.LOGIN_TIME)) {
                    // 将TIME设置为登入时间
                    data.put(DataKey.TIME, data.get(SUBMIT_LOGIN.LOGIN_TIME));
                } else if (data.containsKey(SUBMIT_LOGIN.LOGOUT_TIME)) {
                    // 将TIME设置为登出时间
                    data.put(DataKey.TIME, data.get(SUBMIT_LOGIN.LOGOUT_TIME));
                } else {
                    // 将TIME设置为注册时间
                    data.put(DataKey.TIME, data.get(SUBMIT_LOGIN.REGIST_TIME));
                }
            } else if (CommandType.SUBMIT_TERMSTATUS.equals(cmd)) {
                // 如果是状态信息上报, 则将TIME设置为采集时间(地标)
                data.put(DataKey.TIME, data.get(DataKey._3101_COLLECT_TIME));
            } else if (CommandType.SUBMIT_HISTORY.equals(cmd)) {
                // 如果是补发数据, 则将TIME设置为数据采集时间
                data.put(DataKey.TIME, data.get(DataKey._2000_COLLECT_TIME));
            } else if (CommandType.SUBMIT_CARSTATUS.equals(cmd)) {
                data.put(DataKey.TIME, data.get("3201"));
            } else if (SysDefine.RENTCAR.equals(cmd)) {
                // 租赁数据
                data.put(DataKey.TIME, data.get("4001"));
            } else if (SysDefine.CHARGE.equals(cmd)) {
                // 充电设施数据
                data.put(DataKey.TIME, data.get("4101"));
            }

        }
    }

    private static class ChargeProcessor implements Serializable {

        private static final long serialVersionUID = 2317874526520165477L;
        /**
         * 每辆车的连续充电报文计次
         */
        private final Map<String, Integer> continueChargingCount = Maps.newHashMap();

        /**
         * 连续10次处于充电状态并且车速为0, 则记录充电状态为"1", 否则重置次数为0.
         * @param data
         * @return
         */
        public void fillChargingStatus(@NotNull final Map<String, String> data) {

            final String vid = data.get(DataKey.VEHICLE_ID);

            final String status = data.get(DataKey._2301_CHARGE_STATUS);
            if (StringUtils.isNotEmpty(status)) {

                final String speed = data.get(DataKey._2201_SPEED);
                if (DataKey._2301_CHARGE_STATUS_CHARGING.equals(status)
                    // 车速为0
                    && "0".equals(NumberUtils.isDigits(speed) ? speed : "0")) {

                    final int continueCount = continueChargingCount.getOrDefault(vid, 0);
                    if (continueCount >= 10) {
                        data.put(SysDefine.IS_CHARGE, "1");
                    } else {
                        continueChargingCount.put(vid, continueCount + 1);
                    }
                }
            }

            continueChargingCount.put(vid, 0);
            // 如果不包含充电状态, 则记录充电状态为"0"
            data.put(SysDefine.IS_CHARGE, "0");
        }
    }

    private static class PowerBatteryAlarmFlagProcessor implements Serializable {

        private static final long serialVersionUID = 962973199549443550L;

        /**
         * 北京地标: 动力蓄电池报警标志解析存储, 见表20
         * @param data
         */
        public void fillPowerBatteryAlarm(@NotNull final Map<String, String> data) {

            final short powerBatteryAlarmFlag = NumberUtils.toShort(data.get(DataKey._2801_POWER_BATTERY_ALARM_FLAG_2801), (short) 0);

            // 温度差异报警
            data.put("2901", String.valueOf((powerBatteryAlarmFlag >>> 0) & 1));
            // 电池极柱高温报警
            data.put("2902", String.valueOf((powerBatteryAlarmFlag >>> 1) & 1));
            // 动力蓄电池包过压报警
            data.put("2903", String.valueOf((powerBatteryAlarmFlag >>> 2) & 1));
            // 动力蓄电池包欠压报警
            data.put("2904", String.valueOf((powerBatteryAlarmFlag >>> 3) & 1));
            // SOC低报警
            data.put("2905", String.valueOf((powerBatteryAlarmFlag >>> 4) & 1));
            // 单体蓄电池过压报警
            data.put("2906", String.valueOf((powerBatteryAlarmFlag >>> 5) & 1));
            // 单体蓄电池欠压报警
            data.put("2907", String.valueOf((powerBatteryAlarmFlag >>> 6) & 1));
            // SOC太低报警
            data.put("2908", String.valueOf((powerBatteryAlarmFlag >>> 7) & 1));
            // SOC过高报警
            data.put("2909", String.valueOf((powerBatteryAlarmFlag >>> 8) & 1));
            // 动力蓄电池包不匹配报警
            data.put("2910", String.valueOf((powerBatteryAlarmFlag >>> 9) & 1));
            // 动力蓄电池包不匹配报警
            data.put("2911", String.valueOf((powerBatteryAlarmFlag >>> 10) & 1));
            // 绝缘故障
            data.put("2912", String.valueOf((powerBatteryAlarmFlag >>> 11) & 1));
        }
    }

    private static class AlarmProcessor implements Serializable {

        private static final long serialVersionUID = 9087497673175781559L;

        /**
         * 中国国标: 通用报警标志值, 见表18
         * @param data
         */
        public void fillAlarm(@NotNull final Map<String, String> data) {

            final int alarmMark = NumberUtils.toInt(data.get(DataKey._3801_ALARM_MARK), 0);

            // 温度差异报警
            data.put("2901", String.valueOf((alarmMark >>> 0) & 1));
            // 电池高温报警
            data.put("2902", String.valueOf((alarmMark >>> 1) & 1));
            // 车载储能装置类型过压报警
            data.put("2903", String.valueOf((alarmMark >>> 2) & 1));
            // 车载储能装置类型欠压报警
            data.put("2904", String.valueOf((alarmMark >>> 3) & 1));
            // SOC低报警
            data.put("2905", String.valueOf((alarmMark >>> 4) & 1));
            // 单体电池过压报警
            data.put("2906", String.valueOf((alarmMark >>> 5) & 1));
            // 单体电池欠压报警
            data.put("2907", String.valueOf((alarmMark >>> 6) & 1));
            // SOC过高报警
            data.put("2909", String.valueOf((alarmMark >>> 7) & 1));
            // SOC跳变报警
            data.put("2930", String.valueOf((alarmMark >>> 8) & 1));
            // 可充电储能系统不匹配报警
            data.put("2910", String.valueOf((alarmMark >>> 9) & 1));
            // 电池单体一致性差报警
            data.put("2911", String.valueOf((alarmMark >>> 10) & 1));
            // 绝缘报警
            data.put("2912", String.valueOf((alarmMark >>> 11) & 1));
            // DC-DC温度报警
            data.put("2913", String.valueOf((alarmMark >>> 12) & 1));
            // 制动系统报警
            data.put("2914", String.valueOf((alarmMark >>> 13) & 1));
            // DC-DC状态报警
            data.put("2915", String.valueOf((alarmMark >>> 14) & 1));
            // 驱动电机控制器温度报警
            data.put("2916", String.valueOf((alarmMark >>> 15) & 1));
            // 高压互锁状态报警
            data.put("2917", String.valueOf((alarmMark >>> 16) & 1));
            // 驱动电机温度报警
            data.put("2918", String.valueOf((alarmMark >>> 17) & 1));
            // 车载储能装置类型过充(第18位)
            data.put("2919", String.valueOf((alarmMark >>> 18) & 1));
        }
    }

    private static class StatusFlagsProcessor implements Serializable {

        private static final long serialVersionUID = -7702552036611826880L;

        /**
         * 北京地标: 车载终端状态解析存储, 见表23
         * @param data
         */
        public void fillStatusFlags(@NotNull final Map<String, String> data) {

            final byte statusFlags = NumberUtils.toByte(data.get(DataKey._3110_STATUS_FLAGS), (byte)0);

            // 1-通电, 0-断开
            data.put("3102", String.valueOf((statusFlags >>> 0) & 1));
            // 1-电源正常, 0-电源异常
            data.put("3103", String.valueOf((statusFlags >>> 1) & 1));
            // 1-通信传输正常, 0-通信传输异常
            data.put("3104", String.valueOf((statusFlags >>> 2) & 1));
            // 1-其它正常, 0-其它异常
            data.put("3105", String.valueOf((statusFlags >>> 3) & 1));
        }
    }
}
