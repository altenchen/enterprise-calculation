package storm.bolt.deal.norm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.alarm.AlarmStatus;
import storm.dto.alarm.EarlyWarn;
import storm.dto.alarm.EarlyWarnsGetter;
import storm.extension.ObjectExtension;
import storm.protocol.CommandType;
import storm.stream.KafkaStream;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.DataUtils;
import storm.util.JsonUtils;
import storm.util.ParamsRedisUtil;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author wza
 * 预警处理
 */
public class AlarmBolt extends BaseRichBolt {

    private static final long serialVersionUID = 8457140401762516071L;

    private static final Logger LOG = LoggerFactory.getLogger(AlarmBolt.class);

    // region Component

    @NotNull
    private static final String COMPONENT_ID = AlarmBolt.class.getSimpleName();

    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    // region KafkaStream

    @NotNull
    private static final KafkaStream KAFKA_STREAM = KafkaStream.getInstance();

    @NotNull
    private static final String KAFKA_STREAM_ID = KAFKA_STREAM.getStreamId(COMPONENT_ID);

    @NotNull
    @Contract(pure = true)
    public static String getKafkaStreamId() {
        return KAFKA_STREAM_ID;
    }

    // endregion KafkaStream

    // region 静态常量

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();

    /**
     * 连续多少条报警才发送通知
     */
    private static final int ALARM_TRIGGER_CONTINUE_COUNT;

    /**
     * 同步数据库规则
     */
    private static final long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND;

    /**
     * 离线超时时间
     */
    private static final long VEHICLE_ONLINE_TIMEOUT_MILLISECOND;

    /**
     * 告警消息 kafka 输出 topic
     */
    private static final String VEHICLE_ALARM_TOPIC;

    /**
     * HBase 车辆报警状态存储 kafka 输出 topic
     */
    private static final String VEHICLE_ALARM_STORE_TOPIC;

    static {

        final ConfigUtils configUtils = ConfigUtils.getInstance();
        final Properties sysDefine = configUtils.sysDefine;

        ALARM_TRIGGER_CONTINUE_COUNT = NumberUtils.toInt(
            sysDefine.getProperty(SysDefine.ALARM_START_TRIGGER_CONTINUE_COUNT),
            1);

        DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(
            NumberUtils.toLong(
                sysDefine.getProperty(
                    SysDefine.DB_CACHE_FLUSH_TIME_SECOND),
                60
            )
        );

        VEHICLE_ONLINE_TIMEOUT_MILLISECOND = TimeUnit.SECONDS.toMillis(
            NumberUtils.toLong(
                sysDefine.getProperty(
                    StormConfigKey.REDIS_OFFLINE_SECOND),
                600
            )
        );

        VEHICLE_ALARM_TOPIC = sysDefine.getProperty(
            SysDefine.VEHICLE_ALARM_TOPIC,
            "SYNC_REALTIME_ALARM");
        VEHICLE_ALARM_STORE_TOPIC = sysDefine.getProperty(
            SysDefine.VEHICLE_ALARM_STORE_TOPIC,
            "SYNC_ALARM_STORE");
    }

    // endregion 静态常量

    private transient OutputCollector collector;

    private transient KafkaStream.SenderBuilder kafkaStreamSenderBuilder;

    private transient KafkaStream.Sender kafkaStreamVehicleAlarmSender;

    private transient KafkaStream.Sender kafkaStreamVehicleAlarmStoreSender;

    /**
     * 车辆数据缓存
     * <vid, <key, value>>
     */
    @NotNull
    private transient Map<String, ImmutableMap<String, String>> vehicleCache
        = Maps.newHashMap();

    /**
     * 规则报警缓存
     * <ruleId, <vid, status>>
     */
    @NotNull
    private transient Map<String, Map<String, AlarmStatus>> ruleVehicleStatus =
        Maps.newHashMap();

    @NotNull
    private AlarmStatus ensureStatus(@NotNull final String ruleId, @NotNull final String vehicleId) {
        return ruleVehicleStatus
            .computeIfAbsent(
                ruleId,
                k -> Maps.newHashMap())
            .computeIfAbsent(
                vehicleId,
                AlarmStatus::new);
    }

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        vehicleCache = Maps.newHashMap();
        ruleVehicleStatus = Maps.newHashMap();

        prepareStreamSender(collector);
    }

    private void prepareStreamSender(
        @NotNull final OutputCollector collector) {

        kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        kafkaStreamVehicleAlarmSender = kafkaStreamSenderBuilder.build(VEHICLE_ALARM_TOPIC);
        kafkaStreamVehicleAlarmStoreSender = kafkaStreamSenderBuilder.build(VEHICLE_ALARM_STORE_TOPIC);
    }

    @Override
    public void execute(@NotNull final Tuple input) {

        collector.ack(input);

        if (input.getSourceComponent().equals(FilterBolt.getComponentId())
            && input.getSourceStreamId().equals(SysDefine.SPLIT_GROUP)) {

            final String vehicleId = input.getString(0);

            @SuppressWarnings("unchecked")
            final ImmutableMap<String, String> data = ImmutableMap.copyOf(
                (Map<String, String>) input.getValue(1));

            executeFromDataCacheStream(input, vehicleId, data);

        } else {
            LOG.warn("未处理的流[{}][{}]", input.getSourceComponent(), input.getSourceStreamId());
        }
    }

    private void executeFromDataCacheStream(
        @NotNull final Tuple input,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data) {

        final String messageType = data.get(DataKey.MESSAGE_TYPE);
        if (StringUtils.isBlank(messageType)) {
            return;
        }

        try {
            final ImmutableMap<String, String> cache = ObjectExtension.defaultIfNull(
                vehicleCache.get(vehicleId),
                ImmutableMap::of);

            final long platformReceiveTime;
            try {
                platformReceiveTime = DataUtils.parsePlatformReceiveTime(data);
                final long cacheTime = DataUtils.parsePlatformReceiveTime(cache);
                if(platformReceiveTime < cacheTime) {
                    LOG.warn("VID[{}]平台接收时间乱序, [{}] < [{}]", vehicleId, platformReceiveTime, cacheTime);
                    return;
                }
            } catch (final ParseException e) {
                LOG.warn("解析服务器时间异常", e);
                return;
            }

            processAlarm(input, vehicleId, data, cache, platformReceiveTime, messageType);
        } catch (Exception e) {
            LOG.warn("处理报警出错[{}]", data);
        } finally {
            updateVehicleCache(vehicleId, data, messageType);
        }
    }

    private void processAlarm(
        @NotNull final Tuple input,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        final long platformReceiveTime,
        @NotNull final String messageType) {

        if (!CommandType.SUBMIT_REALTIME.equals(messageType)) {
            return;
        }

        final String vehicleType = data.get(DataKey.VEHICLE_TYPE);
        if (StringUtils.isBlank(vehicleType)) {
            LOG.warn("实时数据没有车型");
            return;
        }

        final ImmutableMap<String, EarlyWarn> rules = EarlyWarnsGetter.getRulesByVehicleModel(vehicleType);

        rules.values().forEach(rule->{
            try {
                final Boolean result = rule.compute(data, cache);
                if(null == result) {
                    return;
                }
                final AlarmStatus alarmStatus = ensureStatus(rule.ruleId, vehicleId);
                alarmStatus.updateVehicleAlarmData(
                    result,
                    platformReceiveTime,
                    rule.ruleId,
                    rule.level,
                    data,
                    rule,
                    notice -> {
                        if (MapUtils.isNotEmpty(notice)) {
                            final String json = JSON_UTILS.toJson(notice);
                            kafkaStreamVehicleAlarmSender.emit(input, vehicleId, json);
                            kafkaStreamVehicleAlarmStoreSender.emit(input, vehicleId, json);
                        }
                    }
                );
            } catch (final Exception e) {
                LOG.warn(
                    "处理平台报警规则[{}][{}]时发生异常[{}][{}]",
                    rule.ruleId,
                    rule.ruleName,
                    data,
                    cache,
                    e);
            }
        });
    }

    private void updateVehicleCache(
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final String messageType) {

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
            vehicleCache.compute(vehicleId, (key, oldValue) -> {
                final HashMap<String, String> newValue = Maps.newHashMap(
                    ObjectExtension.defaultIfNull(
                        vehicleCache.get(vehicleId),
                        ImmutableMap::of));
                data.forEach((k, v) -> {
                    if (StringUtils.isNotBlank(v)) {
                        newValue.put(k, v);
                    }
                });
                return ImmutableMap.copyOf(newValue);
            });
        }
    }

    @Override
    public void declareOutputFields(@NotNull final OutputFieldsDeclarer declarer) {

        KAFKA_STREAM.declareOutputFields(KAFKA_STREAM_ID, declarer);
    }

}