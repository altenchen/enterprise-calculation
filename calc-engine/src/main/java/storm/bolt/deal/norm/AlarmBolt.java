package storm.bolt.deal.norm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
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
import redis.clients.jedis.Jedis;
import storm.dao.DataToRedis;
import storm.dto.alarm.AlarmStatus;
import storm.dto.alarm.CoefficientOffsetGetter;
import storm.dto.alarm.EarlyWarn;
import storm.dto.alarm.EarlyWarnsGetter;
import storm.extension.ObjectExtension;
import storm.protocol.CommandType;
import storm.stream.KafkaStream;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author wza
 * 预警处理
 */
public class AlarmBolt extends BaseRichBolt {

    private static final long serialVersionUID = 8457140401762516071L;

    private static final Logger LOG = LoggerFactory.getLogger(AlarmBolt.class);

    @NotNull
    private static final String COMPONENT_ID = AlarmBolt.class.getSimpleName();

    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    @NotNull
    private static final KafkaStream KAFKA_STREAM = KafkaStream.getInstance();

    @NotNull
    private static final String KAFKA_STREAM_ID = KAFKA_STREAM.getStreamId(COMPONENT_ID);

    @NotNull
    @Contract(pure = true)
    public static String getKafkaStreamId() {
        return KAFKA_STREAM_ID;
    }

    // region 静态常量
    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final DataToRedis redis = new DataToRedis();

    private static final int REDIS_DATABASE_INDEX = 6;

    private static final String IDLE_VEHICLE_REDIS_KEY = "vehCache.qy.alarm";

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();

    private transient OutputCollector collector;

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
                        final ImmutableMap<String, String> startNotice = ImmutableMap.copyOf(
                            ObjectExtension.defaultIfNull(
                                JSON_UTILS.fromJson(
                                    json,
                                    TREE_MAP_STRING_STRING_TYPE,
                                    e -> {
                                        LOG.warn("REDIS DB:{} KEY:{} FIELD:{} 中不是合法json的异常平台报警通知 {}", REDIS_DATABASE_INDEX, IDLE_VEHICLE_REDIS_KEY, field, json);
                                        return null;
                                    }),
                                Maps::newTreeMap));
                        final String status = startNotice.get(AlarmStatus.NOTICE_STATUS_KEY);
                        if(AlarmStatus.NOTICE_STATUS_START.equals(status)) {
                            return new AlarmStatus(vehicleId, true);
                        } else if(AlarmStatus.NOTICE_STATUS_END.equals(status)) {
                            LOG.warn("REDIS DB:{} KEY:{} FIELD:{} 中已结束的平台报警通知 {}", REDIS_DATABASE_INDEX, IDLE_VEHICLE_REDIS_KEY, field, json);
                            jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
                        } else {
                            LOG.warn("REDIS DB:{} KEY:{} FIELD:{} 中状态为 {} 的异常平台报警通知 {}", REDIS_DATABASE_INDEX, IDLE_VEHICLE_REDIS_KEY, field, status, json);
                            jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
                        }
                    }
                    return new AlarmStatus(vehicleId, false);
                }));
    }

    private transient long lastFinishUnableRuleTime;

    @Override
    public void prepare(
            @NotNull final Map stormConf,
            @NotNull final TopologyContext context,
            @NotNull final OutputCollector collector) {
        //首次从redis读取配置
        ConfigUtils.readConfigFromRedis(redis);

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

        final KafkaStream.SenderBuilder kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        kafkaStreamVehicleAlarmSender = kafkaStreamSenderBuilder.build(ConfigUtils.getSysDefine().getKafkaProducerVehicleAlarmTopic());
        kafkaStreamVehicleAlarmStoreSender = kafkaStreamSenderBuilder.build(ConfigUtils.getSysDefine().getKafkaProducerVehicleAlarmStoreTopic());
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

            @SuppressWarnings("unchecked") final ImmutableMap<String, String> data = ImmutableMap.copyOf(
                    (Map<String, String>) input.getValue(1));

            executeFromDataCacheStream(input, vehicleId, data);

            return;
        }

        LOG.warn("未处理的流 {} {}", input.getSourceComponent(), input.getSourceStreamId());
        collector.fail(input);
    }

    private void executeFromSystemTickStream(
            @NotNull final Tuple input) {

        collector.ack(input);

        ConfigUtils.readConfigFromRedis(redis);
        final long currentTimeMillis = System.currentTimeMillis();

        // 每分钟清理一次不可用的平台报警规则未结束通知
        if (currentTimeMillis - lastFinishUnableRuleTime > TimeUnit.MINUTES.toMillis(1)) {

            lastFinishUnableRuleTime = currentTimeMillis;

            final Set<String> enableRuleIds = ImmutableSet.copyOf(
                    EarlyWarnsGetter.getAllRules()
                            .values()
                            .stream()
                            .flatMap(rules -> rules.keySet().stream())
                            .collect(Collectors.toSet())
            );

            JEDIS_POOL_UTILS.useResource(jedis -> {
                jedis.select(REDIS_DATABASE_INDEX);


                ruleVehicleStatus.entrySet().removeIf(next -> {
                    final String ruleId = next.getKey();
                    if (enableRuleIds.contains(ruleId)) {
                        return false;
                    } else {
                        final Map<String, AlarmStatus> vehicleStatus = next.getValue();

                        if (MapUtils.isNotEmpty(vehicleStatus)) {
                            vehicleStatus.forEach((vehicleId, status) -> {
                                if (null != status && BooleanUtils.isTrue(status.getStatus())) {
                                    finishNoticeIfStarted(
                                        jedis,
                                        buildRedisField(vehicleId, ruleId),
                                        notice -> emitNotice(input, vehicleId, ruleId, notice));
                                }
                            });
                        }
                        return true;
                    }
                });

                final Set<String> fields = jedis.hkeys(IDLE_VEHICLE_REDIS_KEY);
                fields.forEach(field -> {
                    final ImmutableList<String> parts = parseRedisField(field);
                    if(REDIS_FIELD_PARTS_COUNT == parts.size()) {
                        final String vehicleId = parts.get(REDIS_FIELD_PARTS_VEHICLE_ID_INDEX);
                        final String ruleId = parts.get(REDIS_FIELD_PARTS_RULE_ID_INDEX);

                        if(vehicleCache.containsKey(vehicleId) && !enableRuleIds.contains(ruleId)) {
                            finishNoticeIfStarted(
                                jedis,
                                field,
                                notice -> emitNotice(input, vehicleId, ruleId, notice));
                        }
                    } else {
                        LOG.warn("REDIS DB:{} KEY:{} 无效的平台报警通知键 {}", REDIS_DATABASE_INDEX, IDLE_VEHICLE_REDIS_KEY, field);
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

        if (!CommandType.SUBMIT_REALTIME.equals(messageType)) {
            return;
        }

        try {
            final ImmutableMap<String, String> cache = ObjectExtension.defaultIfNull(
                    vehicleCache.get(vehicleId),
                    ImmutableMap::of);

            final long platformReceiveTime;
            try {
                platformReceiveTime = DataUtils.parsePlatformReceiveTime(data);
                if (MapUtils.isNotEmpty(cache)) {
                    final long cacheTime = DataUtils.parsePlatformReceiveTime(cache);
                    if (platformReceiveTime < cacheTime) {
                        LOG.warn("VID:{} 平台接收时间乱序, {} < {}", vehicleId, platformReceiveTime, cacheTime);
                        return;
                    }
                }
            } catch (final ParseException e) {
                LOG.warn("VID:" + vehicleId + " 解析服务器时间异常", e);
                return;
            }

            processAlarm(input, vehicleId, data, cache, platformReceiveTime);
        } catch (Exception e) {
            LOG.warn("VID:" + vehicleId + " 处理报警出错 " + data, e);
        } finally {
            updateVehicleCache(vehicleId, data, messageType);
        }
    }

    private void processAlarm(
            @NotNull final Tuple input,
            @NotNull final String vehicleId,
            @NotNull final ImmutableMap<String, String> data,
            @NotNull final ImmutableMap<String, String> cache,
            final long platformReceiveTime) {

        final String vehicleType = data.get(DataKey.VEHICLE_TYPE);
        if (StringUtils.isBlank(vehicleType)) {
            LOG.warn("VID:{} 实时数据没有车型 {}", vehicleId, JSON_UTILS.toJson(data));
            return;
        }

        final ImmutableMap<String, EarlyWarn> rules = EarlyWarnsGetter.getRulesByVehicleModel(vehicleType);

        rules.values().forEach(rule -> {
            try {
                final Boolean result = rule.compute(data, cache);
                if (null == result) {
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
                LOG.warn("VID:{} 处理平台报警规则 RULE_ID:{} ROLE_NAME:{} 时发生异常 DATA:{} CACHE:{}", vehicleId, rule.ruleId, rule.ruleName, data, cache, e);
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

    private void finishNoticeIfStarted(
        @NotNull final Jedis jedis,
        @NotNull final String field,
        @NotNull final Consumer<ImmutableMap<String, String>> noticeCallback) {

        final String json = jedis.hget(IDLE_VEHICLE_REDIS_KEY, field);
        // 如果 redis 中有未结束状态, 则加载未结束状态并结束
        if (StringUtils.isNotBlank(json)) {
            final ImmutableMap<String, String> startNotice = ImmutableMap.copyOf(
                ObjectExtension.defaultIfNull(
                    JSON_UTILS.fromJson(
                        json,
                        TREE_MAP_STRING_STRING_TYPE,
                        e -> {
                            LOG.warn("REDIS DB:{} KEY:{} FIELD:{} 中不是合法json的异常平台报警通知 {}", REDIS_DATABASE_INDEX, IDLE_VEHICLE_REDIS_KEY, field, json);
                            return null;
                        }),
                    Maps::newTreeMap));
            final String status = startNotice.get(AlarmStatus.NOTICE_STATUS_KEY);
            if(AlarmStatus.NOTICE_STATUS_START.equals(status)) {
                finishNoticeIfStarted(
                    startNotice,
                    noticeCallback
                );
            } else if(AlarmStatus.NOTICE_STATUS_END.equals(status)) {
                LOG.warn("REDIS DB:{} KEY:{} FIELD:{} 中已结束的平台报警通知 {}", REDIS_DATABASE_INDEX, IDLE_VEHICLE_REDIS_KEY, field, json);
                jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
            } else if(MapUtils.isNotEmpty(startNotice)) {
                LOG.warn("REDIS DB:{} KEY:{} FIELD:{} 中状态为 {} 的异常平台报警通知 {}", REDIS_DATABASE_INDEX, IDLE_VEHICLE_REDIS_KEY, field, status, json);
                jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
            }
        } else {
            LOG.warn("REDIS DB:{} KEY:{} FIELD:{} 中为null的异常平台报警通知", REDIS_DATABASE_INDEX, IDLE_VEHICLE_REDIS_KEY, field);
            jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
        }
    }

    private void finishNoticeIfStarted(
        @NotNull final ImmutableMap<String, String> startNotice,
        @NotNull final Consumer<ImmutableMap<String, String>> noticeCallback) {
        if(MapUtils.isNotEmpty(startNotice)) {
            final Map<String, String> endNotice = Maps.newHashMap(startNotice);
            endNotice.put("STATUS", AlarmStatus.NOTICE_STATUS_END);
            endNotice.put("eNoticeTime", DataUtils.buildFormatTime(System.currentTimeMillis()));
            endNotice.put("reason", "rule_unable");

            noticeCallback.accept(ImmutableMap.copyOf(endNotice));
        }
    }

    private void emitNotice(
            @NotNull final Tuple input,
            @NotNull final String vehicleId,
            @NotNull final String ruleId,
            @Nullable final ImmutableMap<String, String> notice) {

        if (MapUtils.isNotEmpty(notice)) {
            final String json = JSON_UTILS.toJson(notice);
            LOG.info("输出平台报警通知 {} ", json);

            kafkaStreamVehicleAlarmSender.emit(input, vehicleId, json);
            kafkaStreamVehicleAlarmStoreSender.emit(input, vehicleId, json);


            JEDIS_POOL_UTILS.useResource(jedis -> {
                jedis.select(REDIS_DATABASE_INDEX);

                final String field = buildRedisField(vehicleId, ruleId);
                final String status = notice.get(AlarmStatus.NOTICE_STATUS_KEY);
                if (AlarmStatus.NOTICE_STATUS_START.equals(status)) {
                    jedis.hset(IDLE_VEHICLE_REDIS_KEY, field, json);
                } else if (AlarmStatus.NOTICE_STATUS_END.equals(status)) {
                    jedis.hdel(IDLE_VEHICLE_REDIS_KEY, field);
                }
            });
        }
    }

    private static final int REDIS_FIELD_PARTS_COUNT = 2;
    private static final int REDIS_FIELD_PARTS_VEHICLE_ID_INDEX = 0;
    private static final int REDIS_FIELD_PARTS_RULE_ID_INDEX = 1;

    @NotNull
    @Contract(pure = true)
    private String buildRedisField(
            @NotNull final String vehicleId,
            @NotNull final String ruleId) {
        return vehicleId + "_" + ruleId;
    }

    private ImmutableList<String> parseRedisField(final String field) {
        final String[] parts = StringUtils.split(field, '_');
        if(null != parts && REDIS_FIELD_PARTS_COUNT == parts.length) {
            return ImmutableList.of(
                parts[REDIS_FIELD_PARTS_VEHICLE_ID_INDEX],
                parts[REDIS_FIELD_PARTS_RULE_ID_INDEX]);
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