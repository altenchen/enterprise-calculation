package storm.bolt.deal.norm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
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
import storm.cache.VehicleCache;
import storm.dao.DataToRedis;
import storm.debug.DebugEmitSpout;
import storm.extension.ImmutableMapExtension;
import storm.extension.MapExtension;
import storm.handler.cusmade.TimeOutOfRangeNotice;
import storm.handler.cusmade.VehicleIdleHandler;
import storm.kafka.bolt.KafkaBoltTopic;
import storm.kafka.spout.GeneralKafkaSpout;
import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LOGIN;
import storm.spout.MySqlSpout;
import storm.stream.DataCacheStream;
import storm.stream.DataStream;
import storm.stream.KafkaStream;
import storm.stream.StreamReceiverFilter;
import storm.system.DataKey;
import storm.system.ProtocolItem;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.text.ParseException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

    // region Component

    @NotNull
    private static final String COMPONENT_ID = FilterBolt.class.getSimpleName();

    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    // region DataStream

    @NotNull
    private static final DataStream DATA_STREAM = DataStream.getInstance();

    @NotNull
    private static final String DATA_STREAM_ID = DATA_STREAM.getStreamId(COMPONENT_ID);

    @NotNull
    @Contract(pure = true)
    public static String getDataStreamId() {
        return DATA_STREAM_ID;
    }

    @NotNull
    public static StreamReceiverFilter prepareDataStreamReceiver(
        @NotNull final DataStream.IProcessor processor) {

        return DATA_STREAM
            .prepareReceiver(
                processor)
            .filter(
                COMPONENT_ID,
                DATA_STREAM_ID);
    }

    // endregion DataStream

    // region DataCacheStream

    @NotNull
    private static final DataCacheStream DATA_CACHE_STREAM = DataCacheStream.getInstance();

    @NotNull
    private static final String DATA_CACHE_STREAM_ID = DATA_CACHE_STREAM.getStreamId(COMPONENT_ID);

    @NotNull
    @Contract(pure = true)
    public static String getDataCacheStreamId() {
        return DATA_CACHE_STREAM_ID;
    }

    @NotNull
    public static StreamReceiverFilter prepareDataCacheStreamReceiver(
        @NotNull final DataCacheStream.IProcessor processor) {

        return DATA_CACHE_STREAM
            .prepareReceiver(
                processor)
            .filter(
                COMPONENT_ID,
                DATA_CACHE_STREAM_ID);
    }

    // endregion DataCacheStream

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

    private static final JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    private static final DataToRedis redis = new DataToRedis();

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    private static final Pattern MSG_REGEX = Pattern.compile("^([^ ]+) (\\d+) ([^ ]+) ([^ ]+) \\{VID:([^,]*)(?:,([^}]+))*\\}$");

    static {
        if (LOG.isInfoEnabled()) {
            LOG.info("时间异常通知已{}.", ConfigUtils.getSysDefine().isNoticeTimeEnable() ? "启用" : "禁用");
        }
        TimeOutOfRangeNotice.setTimeRangeMillisecond(ConfigUtils.getSysDefine().getNoticeTimeRangeAbsMillisecond());
    }

    // endregion 类常量

    // region 对象变量

    private transient TimeOutOfRangeNotice timeOutOfRangeNotice;

    private transient OnlineProcessor onlineProcessor;

    private transient TimeProcessor timeProcessor;

    private transient ChargeProcessor chargeProcessor;

    private transient PowerBatteryAlarmFlagProcessor powerBatteryAlarmFlagProcessor;

    private transient AlarmProcessor alarmProcessor;

    private transient StatusFlagsProcessor statusFlagsProcessor;

    /**
     * 闲置车辆处理
     */
    private transient VehicleIdleHandler vehicleIdleHandler;

    private transient long lastComputIdleTime;

    private transient OutputCollector collector;

    private transient Map<String, ImmutableMap<String, String>> vehicleCache;

    private transient DataStream.BoltSender dataStreamSender;

    private transient DataCacheStream.BoltSender dataCacheStreamSender;

    private transient KafkaStream.Sender kafkaStreamVehicleNoticeSender;

    private transient StreamReceiverFilter kafkaGeneralStreamReceiver;

    private transient StreamReceiverFilter debugGeneralStreamReceiver;

    private transient StreamReceiverFilter ctfoBoltDataStreamReceiver;

    // endregion 对象变量

    // region IComponent

    @Override
    public void declareOutputFields(@NotNull final OutputFieldsDeclarer declarer) {

        DATA_STREAM.declareOutputFields(DATA_STREAM_ID, declarer);

        DATA_CACHE_STREAM.declareOutputFields(DATA_CACHE_STREAM_ID, declarer);

        KAFKA_STREAM.declareOutputFields(KAFKA_STREAM_ID, declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        final Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return config;
    }

    // endregion IComponent

    // region IBolt

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {
        //将storm启动时的自定义参数设置进来
        LOG.info("将STORM启动时设置的参数填充进来");
        ConfigUtils.fillSysDefineEntity(stormConf);
        //首次从redis读取配置
        LOG.info("将REDIS动态设置的参数填充进来");
        ConfigUtils.readConfigFromRedis(redis);

        this.collector = collector;

        vehicleCache = Maps.newHashMap();

        timeOutOfRangeNotice = new TimeOutOfRangeNotice();
        onlineProcessor = new OnlineProcessor();
        timeProcessor = new TimeProcessor();
        chargeProcessor = new ChargeProcessor();
        powerBatteryAlarmFlagProcessor = new PowerBatteryAlarmFlagProcessor();
        alarmProcessor = new AlarmProcessor();
        statusFlagsProcessor = new StatusFlagsProcessor();

        vehicleIdleHandler = new VehicleIdleHandler();
        vehicleIdleHandler.setIdleTimeoutMillisecond(ConfigUtils.getSysDefine().getVehicleIdleTimeoutMillisecond());

        vehicleCache = Maps.newHashMap();

        prepareStreamSender(collector);

        prepareStreamReceiver();
    }

    // region prepare

    private void prepareStreamSender(
        @NotNull final OutputCollector collector) {

        dataStreamSender = DATA_STREAM.prepareSender(DATA_STREAM_ID, collector);

        dataCacheStreamSender = DATA_CACHE_STREAM.prepareSender(DATA_CACHE_STREAM_ID, collector);

        final KafkaStream.SenderBuilder kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        kafkaStreamVehicleNoticeSender = kafkaStreamSenderBuilder.build(KafkaBoltTopic.NOTICE_TOPIC);
    }

    private void prepareStreamReceiver() {

        kafkaGeneralStreamReceiver = GeneralKafkaSpout.prepareGeneralStreamReceiver(this::executeFromGeneralStream);

        debugGeneralStreamReceiver = DebugEmitSpout.prepareGeneralStreamReceiver(this::executeFromGeneralStream);

        ctfoBoltDataStreamReceiver = MySqlSpout.prepareVehicleIdentityStreamReceiver(this::executeFromMySqlSpoutVehicleIdentityStream);
    }

    // endregion prepare

    @Override
    public void execute(
        @NotNull final Tuple input) {

        if (TupleUtils.isTick(input)) {
            executeFromSystemTickStream(input);
            return;
        }

        if (kafkaGeneralStreamReceiver.execute(input)) {
            return;
        }

        if (ctfoBoltDataStreamReceiver.execute(input)) {
            return;
        }

        if (debugGeneralStreamReceiver.execute(input)) {
            return;
        }

        collector.fail(input);
    }

    // region execute

    private void executeFromSystemTickStream(
        @NotNull final Tuple input) {

        collector.ack(input);

        ConfigUtils.readConfigFromRedis(redis);
        final long currentTimeMillis = System.currentTimeMillis();

        // 每分钟计算一次闲置车辆
        if(currentTimeMillis - lastComputIdleTime > TimeUnit.MINUTES.toMillis(1)) {

            lastComputIdleTime = currentTimeMillis;

            vehicleIdleHandler.setIdleTimeoutMillisecond(ConfigUtils.getSysDefine().getVehicleIdleTimeoutMillisecond());

            vehicleIdleHandler
                .computeStartNotice()
                .forEach((vehicleId, json) -> {
                    kafkaStreamVehicleNoticeSender.emit(input, vehicleId, json);
                });
        }

    }

    @SuppressWarnings("AlibabaMethodTooLong")
    private void executeFromGeneralStream(
        @NotNull final Tuple input,
        @NotNull final String vehicleId,
        @NotNull final String frame) {
        collector.ack(input);

        try{
            final Matcher matcher = MSG_REGEX.matcher(frame);
            if (!matcher.find()) {
                LOG.warn("无效的元组:{} 原始数据:{}", input.toString(), frame);
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

            if(!StringUtils.equals(vehicleId, vid)) {
                LOG.error("分组VID:{} 与解析VID:{} 不一致 原始数据: {}", vehicleId, vid, frame);
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

            final boolean isRealtimeInfo = CommandType.SUBMIT_REALTIME.equals(cmd);
            if (isRealtimeInfo) {

                // 时间异常判断
                final Map<String, String> notice = timeOutOfRangeNotice.process(data);
                if(MapUtils.isNotEmpty(notice)) {
                    if (ConfigUtils.getSysDefine().isNoticeTimeEnable()) {
                        sendNotice(vid, notice);
                    }
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

            // 增加utc字段，插入数据进入 storm 的时间, 这个值可能并不好使, 集群主机如果时间有误差的话.....
            // TODO: 使用服务器时间取代
            data.put(SysDefine.ONLINE_UTC, String.valueOf(System.currentTimeMillis()));

            // 计算在线状态(10002)和平台注册通知类型(TYPE)
            onlineProcessor.fillIsOnline(cmd, data);

            // 计算时间(TIME)加入data
            timeProcessor.fillTime(cmd, data);

            MapExtension.clearNullEntry(data);
            final ImmutableMap<String, String> immutableData = ImmutableMap.copyOf(data);

            emit(input, vid, cmd, immutableData);

            if (isRealtimeInfo) {
                VEHICLE_CACHE.updateUsefulCache(immutableData);

                final String platformTimeString = immutableData.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
                try {

                    final long platformTime = DataUtils.parseFormatTime(platformTimeString);
                    if (platformTime > 0) {
                        vehicleIdleHandler.updatePlatformReceiveTime(vid, platformTime)
                            .forEach((key, json) -> {
                                kafkaStreamVehicleNoticeSender.emit(input, key, json);
                            });
                    }
                } catch (final ParseException e) {
                    LOG.warn("时间解析异常", e);
                    LOG.warn("VID:{} 无效的服务器接收时间:{}", vehicleId, platformTimeString);
                }
            }
        }catch (final Exception e){
            LOG.warn("预处理异常", e);
            e.printStackTrace();
        }
    }

    private void executeFromMySqlSpoutVehicleIdentityStream(
            @NotNull final Tuple input,
            @NotNull final String vehicleId) {

        collector.ack(input);

        try {
            final ImmutableMap<String, String> totalMileageCache = VEHICLE_CACHE.getField(
                    vehicleId,
                    VehicleCache.TOTAL_MILEAGE_FIELD);
            final String totalMileageCacheTimeString = totalMileageCache.get(VehicleCache.VALUE_TIME_KEY);
            if (StringUtils.isNotBlank(totalMileageCacheTimeString)) {
                try {
                    final long platformReceiveTime = DataUtils.parseFormatTime(totalMileageCacheTimeString);

                    if (platformReceiveTime > 0) {
                        vehicleIdleHandler.initPlatformReceiveTime(vehicleId, platformReceiveTime)
                                .forEach((vid, json) -> {
                                    kafkaStreamVehicleNoticeSender.emit(input, vid, json);
                                });
                    }
                } catch (final ParseException e) {
                    LOG.warn("累计里程缓存时间解析异常", e);
                    LOG.warn("VID:{} 无效的累计里程缓存时间: {}", vehicleId, totalMileageCacheTimeString);
                }
            }
        } catch (final ExecutionException e) {
            LOG.warn("VID:" + vehicleId + " 从缓存获取有效累计里程异常", e);
        }
    }

    // endregion execute

    // endregion IBolt

    private void emit(
        @NotNull final Tuple anchors,
        @NotNull final String vehicleId,
        @NotNull final String cmd,
        @NotNull final ImmutableMap<String, String> data) {

        if (CommandType.SUBMIT_REALTIME.equals(cmd)
            || CommandType.SUBMIT_LINKSTATUS.equals(cmd)
            || CommandType.SUBMIT_LOGIN.equals(cmd)
            || CommandType.SUBMIT_TERMSTATUS.equals(cmd)
            || CommandType.SUBMIT_CARSTATUS.equals(cmd)) {

            dataStreamSender.emit(
                anchors,
                vehicleId,
                data);

            dataCacheStreamSender.emit(
                anchors,
                vehicleId,
                data,
                vehicleCache.getOrDefault(
                    vehicleId,
                    ImmutableMap.of()
                )
            );

            vehicleCache.compute(
                vehicleId,
                (k, v) -> Optional
                    .ofNullable(v)
                    .map(old ->
                        ImmutableMapExtension.union(
                            old,
                            data
                        )
                    )
                    .orElse(data)
            );
        }
    }

    private void sendNotice(
        @NotNull final String vid,
        @Nullable final Map<String, String> notice) {

        if(MapUtils.isEmpty(notice)){
            return;
        }

        final String json = JSON_UTILS.toJson(notice);

        kafkaStreamVehicleNoticeSender.emit(vid, json);
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

    private static class OnlineProcessor {

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

    private static class TimeProcessor {

        /**
         * 计算时间(TIME)加入data
         * @param cmd
         * @param data
         */
        public void fillTime(@NotNull final String cmd, @NotNull final Map<String, String> data) {

            // TODO: 统一使用平台接收数据时间

            if (CommandType.SUBMIT_REALTIME.equals(cmd)) {
                // 如果是实时数据, 则将TIME设置为数据采集时间
                data.put(DataKey.TIME, data.get(DataKey._9999_PLATFORM_RECEIVE_TIME));
            } else if (CommandType.SUBMIT_LOGIN.equals(cmd)) {
                // 由于网络不稳定导致断线重连的情况, 这类报文歧义不小.

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
            } else if (CommandType.SUBMIT_CARSTATUS.equals(cmd)) {
                // 车辆运行状态, 采集时间
                data.put(DataKey.TIME, data.get("3201"));
            }

            if (data.get(DataKey.TIME) == null) {
                data.remove(DataKey.TIME);
            }
        }
    }

    private static class ChargeProcessor {

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
                    return;
                }
            }

            continueChargingCount.put(vid, 0);
            // 如果不包含充电状态, 则记录充电状态为"0"
            data.put(SysDefine.IS_CHARGE, "0");
        }
    }

    private static class PowerBatteryAlarmFlagProcessor {

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

    private static class AlarmProcessor {

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

    private static class StatusFlagsProcessor {

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
