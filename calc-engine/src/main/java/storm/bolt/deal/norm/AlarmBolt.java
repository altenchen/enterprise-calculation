package storm.bolt.deal.norm;

import com.google.common.collect.*;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.alarm.AlarmStatus;
import storm.dto.alarm.CoefficientOffsetGetter;
import storm.dto.alarm.EarlyWarn;
import storm.dto.alarm.EarlyWarnsGetter;
import storm.extension.ObjectExtension;
import storm.protocol.CommandType;
import storm.stream.KafkaStream;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.*;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    private static final int REDIS_DATABASE_INDEX = 6;

    private static final String IDLE_VEHICLE_REDIS_KEY = "vehCache.qy.alarm";

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();

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
                k -> JEDIS_POOL_UTILS.useResource(jedis -> {
                    jedis.select(REDIS_DATABASE_INDEX);

                    final String field = buildRedisField(vehicleId, ruleId);
                    final String json = jedis.hget(IDLE_VEHICLE_REDIS_KEY, field);
                    // 如果 redis 中有未结束状态, 则加载未结束状态初始化
                    if (StringUtils.isNotBlank(json)) {
                        final TreeMap<String, String> startNotice = JSON_UTILS.fromJson(
                            json,
                            TREE_MAP_STRING_STRING_TYPE);
                        final String status = startNotice.get(AlarmStatus.NOTICE_STATUS_KEY);
                        if(AlarmStatus.NOTICE_STATUS_START.equals(status)) {
                            return new AlarmStatus(vehicleId, ImmutableMap.copyOf(startNotice));
                        } else if(AlarmStatus.NOTICE_STATUS_END.equals(status)) {
                            // 顺手清理下已结束未删除的状态
                            jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
                        } else {
                            LOG.warn("redis中状态未知的平台报警通知[{}]", json);
                        }
                    }
                    return new AlarmStatus(vehicleId);
                }));
    }

    private transient long lastFinishUnableRuleTime;

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        // region 初始化规则
        EarlyWarnsGetter.getAllRules();
        CoefficientOffsetGetter.getAllCoefficientOffsets();
        // endregion 初始化规则

        final long currentTimeMillis = System.currentTimeMillis();

        this.collector = collector;

        vehicleCache = Maps.newHashMap();
        ruleVehicleStatus = Maps.newHashMap();

        prepareStreamSender(collector);

        lastFinishUnableRuleTime = currentTimeMillis;
    }

    private void prepareStreamSender(
        @NotNull final OutputCollector collector) {

        kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        kafkaStreamVehicleAlarmSender = kafkaStreamSenderBuilder.build(VEHICLE_ALARM_TOPIC);
        kafkaStreamVehicleAlarmStoreSender = kafkaStreamSenderBuilder.build(VEHICLE_ALARM_STORE_TOPIC);
    }

    @Override
    public void execute(@NotNull final Tuple input) {

        if (TupleUtils.isTick(input)) {
            executeFromSystemTickStream(input);
            return;
        }

        if (input.getSourceComponent().equals(FilterBolt.getComponentId())
            && input.getSourceStreamId().equals(SysDefine.SPLIT_GROUP)) {

            final String vehicleId = input.getString(0);

            @SuppressWarnings("unchecked")
            final ImmutableMap<String, String> data = ImmutableMap.copyOf(
                (Map<String, String>) input.getValue(1));

            executeFromDataCacheStream(input, vehicleId, data);

            return;
        }

        LOG.warn("未处理的流[{}][{}]", input.getSourceComponent(), input.getSourceStreamId());
        collector.fail(input);
    }

    private void executeFromSystemTickStream(
        @NotNull final Tuple input) {

        collector.ack(input);

        final long currentTimeMillis = System.currentTimeMillis();

        // 每分钟清理一次不可用的平台报警规则未结束通知
        if(currentTimeMillis - lastFinishUnableRuleTime > TimeUnit.MINUTES.toMillis(1)) {

            lastFinishUnableRuleTime = currentTimeMillis;

            final Set<String> enableRuleIds = ImmutableSet.copyOf(
                EarlyWarnsGetter.getAllRules()
                    .values()
                    .stream()
                    .flatMap(rules -> rules.keySet().stream())
                    .collect(Collectors.toSet())
            );

            ruleVehicleStatus.keySet().forEach(ruleId -> {
                if(!enableRuleIds.contains(ruleId)) {
                    final Map<String, AlarmStatus> vehicleStatus = ruleVehicleStatus.remove(ruleId);
                    if (MapUtils.isNotEmpty(vehicleStatus)) {
                        vehicleStatus.forEach((vehicleId, status)->
                            status.finishNoticeIfStarted(
                                notice -> emitNotice(input, vehicleId, ruleId, notice))
                        );
                    }
                }
            });

            JEDIS_POOL_UTILS.useResource(jedis -> {
                jedis.select(REDIS_DATABASE_INDEX);

                final Set<String> fields = jedis.hkeys(IDLE_VEHICLE_REDIS_KEY);
                fields.forEach(field ->{
                    final ImmutableList<String> parts = parseRedisField(field);
                    if(2 == parts.size()) {
                        final String vehicleId = parts.get(0);
                        final String ruleId = parts.get(1);

                        if(!enableRuleIds.contains(ruleId)) {

                            final String json = jedis.hget(IDLE_VEHICLE_REDIS_KEY, field);
                            // 如果 redis 中有未结束状态, 则加载未结束状态并结束
                            if (StringUtils.isNotBlank(json)) {
                                final TreeMap<String, String> startNotice = JSON_UTILS.fromJson(
                                    json,
                                    TREE_MAP_STRING_STRING_TYPE);
                                final String status = startNotice.get(AlarmStatus.NOTICE_STATUS_KEY);
                                if(AlarmStatus.NOTICE_STATUS_START.equals(status)) {
                                    final AlarmStatus alarmStatus = new AlarmStatus(vehicleId, ImmutableMap.copyOf(startNotice));
                                    alarmStatus.finishNoticeIfStarted(notice-> emitNotice(input, vehicleId, ruleId, notice));
                                } else if(AlarmStatus.NOTICE_STATUS_END.equals(status)) {
                                    // 顺手清理下已结束未删除的状态
                                    jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
                                } else {
                                    LOG.warn("redis中状态未知的平台报警通知[{}]", json);
                                }
                            } else {
                                jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
                            }
                        }
                    } else {
                        jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
                    }
                });
            });
        }
    }

    private void executeFromDataCacheStream(
        @NotNull final Tuple input,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data) {

        collector.ack(input);

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
                    notice -> emitNotice(input, vehicleId, rule.ruleId, notice)
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

    private void emitNotice(
        @NotNull final Tuple input,
        @NotNull final String vehicleId,
        @NotNull final String ruleId,
        @Nullable final ImmutableMap<String, String> notice) {

        if (MapUtils.isNotEmpty(notice)) {
            final String json = JSON_UTILS.toJson(notice);
            kafkaStreamVehicleAlarmSender.emit(input, vehicleId, json);
            kafkaStreamVehicleAlarmStoreSender.emit(input, vehicleId, json);

            JEDIS_POOL_UTILS.useResource(jedis -> {
                jedis.select(REDIS_DATABASE_INDEX);

                final String field = buildRedisField(vehicleId, ruleId);
                final String status = notice.get(AlarmStatus.NOTICE_STATUS_KEY);
                if(AlarmStatus.NOTICE_STATUS_START.equals(status)) {
                    jedis.hset(IDLE_VEHICLE_REDIS_KEY, field, json);
                } else if(AlarmStatus.NOTICE_STATUS_END.equals(status)) {
                    jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
                }
            });
        }
    }

    @NotNull
    @Contract(pure = true)
    private String buildRedisField(
        @NotNull final String vehicleId,
        @NotNull final String ruleId) {
        return vehicleId + "_" + ruleId;
    }

    private ImmutableList<String> parseRedisField(final String field) {
        final String[] parts = StringUtils.split(field, '_');
        if(null != parts && 2 == parts.length) {
            return ImmutableList.of(parts[0], parts[1]);
        } else {
            return ImmutableList.of();
        }
    }

    @Override
    public void declareOutputFields(@NotNull final OutputFieldsDeclarer declarer) {

        KAFKA_STREAM.declareOutputFields(KAFKA_STREAM_ID, declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        final Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return config;
    }

}