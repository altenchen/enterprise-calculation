package storm.bolt.deal.cusmade;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
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
import storm.bolt.deal.norm.FilterBolt;
import storm.cache.SysRealDataCache;
import storm.cache.VehicleCache;
import storm.handler.FaultCodeHandler;
import storm.handler.cusmade.CarOnOffHandler;
import storm.handler.cusmade.CarRuleHandler;
import storm.handler.cusmade.ScanRange;
import storm.protocol.CommandType;
import storm.stream.KafkaStream;
import storm.stream.StreamReceiverFilter;
import storm.system.DataKey;
import storm.system.StormConfigKey;
import storm.system.SysDefine;
import storm.util.DataUtils;
import storm.util.JsonUtils;
import storm.util.ParamsRedisUtil;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author xzp
 */
public final class CarNoticeBolt extends BaseRichBolt {

    private static final long serialVersionUID = -1010194368397854277L;

    private static final Logger LOG = LoggerFactory.getLogger(CarNoticeBolt.class);

    // region Component

    @NotNull
    private static final String COMPONENT_ID = CarNoticeBolt.class.getSimpleName();

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

    private static final ParamsRedisUtil PARAMS_REDIS_UTIL = ParamsRedisUtil.getInstance();

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    private OutputCollector collector;

    private StreamReceiverFilter dataStreamReceiver;

    private KafkaStream.SenderBuilder kafkaStreamSenderBuilder;

    private KafkaStream.Sender kafkaStreamVehicleNoticeSender;

    /**
     * 闲置车辆判定, 达到闲置状态时长, 默认1天
     */
    private long idleTimeoutMillisecond = 86400000L;
    /**
     * 离线检查, 多长时间检查一下是否离线, 默认2分钟
     */
    private long offlineCheckSpanMillisecond = 120000L;
    /**
     * 离线判定, 多长时间算是离线, 默认10分钟
     */
    private long offlineTimeMillisecond = 600000L;
    /**
     * 最后进行离线检查的时间, 用于离线判断
     */
    private long lastOfflineCheckTimeMillisecond;

    /**
     * 车辆规则处理
     */
    private transient CarRuleHandler carRuleHandler;

    /**
     * 车辆上下线及相关处理
     */
    private transient CarOnOffHandler carOnOffhandler;

    /**
     * 故障码处理
     */
    private transient FaultCodeHandler faultCodeHandler;

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        carRuleHandler = new CarRuleHandler();
        carOnOffhandler = new CarOnOffHandler();
        faultCodeHandler = new FaultCodeHandler();

        prepareStreamSender(stormConf, collector);

        prepareStreamReceiver();

        prepareIdleVehicleThread(stormConf);
    }

    private void prepareStreamSender(
        @NotNull final Map stormConf,
        @NotNull final OutputCollector collector) {

        kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        // 输出到Kafka的主题
        final String noticeTopic = stormConf.get(SysDefine.KAFKA_TOPIC_NOTICE).toString();

        kafkaStreamVehicleNoticeSender = kafkaStreamSenderBuilder.build(noticeTopic);
    }

    private void prepareStreamReceiver() {

        dataStreamReceiver = FilterBolt.prepareDataStreamReceiver(this::executeFromDataStream);
    }

    @SuppressWarnings("AlibabaMethodTooLong")
    private void prepareIdleVehicleThread(
        @NotNull final Map stormConf) {

        final long currentTimeMillis = System.currentTimeMillis();
        lastOfflineCheckTimeMillisecond = currentTimeMillis;

        final ParamsRedisUtil paramsRedisUtil = ParamsRedisUtil.getInstance();
        paramsRedisUtil.rebulid();
        // 从Redis读取车辆闲置超时时间
        final Object idleTimeoutMillisecond = paramsRedisUtil.PARAMS.get(
            ParamsRedisUtil.VEHICLE_IDLE_TIMEOUT_MILLISECOND);
        if (null != idleTimeoutMillisecond) {
            this.idleTimeoutMillisecond = (int) idleTimeoutMillisecond;
        }

        // 多长时间算是离线
        final String offLineSecond = MapUtils.getString(stormConf, StormConfigKey.REDIS_OFFLINE_SECOND);
        if (!StringUtils.isEmpty(offLineSecond)) {
            offlineTimeMillisecond = Long.parseLong(org.apache.commons.lang.math.NumberUtils.isNumber(offLineSecond) ? offLineSecond : "0") * 1000;
        }

        // 多长时间检查一下是否离线
        final String offLineCheckSpanSecond = MapUtils.getString(stormConf, StormConfigKey.REDIS_OFFLINE_CHECK_SPAN_SECOND);
        if (!StringUtils.isEmpty(offLineCheckSpanSecond)) {
            offlineCheckSpanMillisecond = Long.parseLong(org.apache.commons.lang.math.NumberUtils.isNumber(offLineCheckSpanSecond) ? offLineCheckSpanSecond : "0") * 1000;
        }

        //闲置车辆判断，发送闲置车辆通知
        try {
            SysRealDataCache.init();

            // region 如果从配置读到 isPrepareCarProcess 为2, 则进行一次全量数据扫描, 并将告警数据发送到kafka

            if (stormConf.containsKey(StormConfigKey.REDIS_CLUSTER_DATA_SYN)) {
                final String isPrepareCarProcessFromRedis = ObjectUtils.toString(stormConf.get(StormConfigKey.REDIS_CLUSTER_DATA_SYN));
                if (StringUtils.isNotBlank(isPrepareCarProcessFromRedis)) {
                    final int isPrepareCarProcess = NumberUtils.toInt(isPrepareCarProcessFromRedis, 0);

                    // 2代表着读取历史车辆数据，即全部车辆, 默认配置为1, 不会触发全量扫描.
                    if (2 == isPrepareCarProcess) {
                        carOnOffhandler.onOffCheck("TIMEOUT", 0, currentTimeMillis, offlineTimeMillisecond);
                        final List<Map<String, Object>> notices = carOnOffhandler.fullDoesNotice("TIMEOUT", ScanRange.AllData, currentTimeMillis, this.idleTimeoutMillisecond);
                        if (CollectionUtils.isNotEmpty(notices)) {
                            LOG.info("---------------syn redis cluster data--------");
                            for (final Map<String, Object> notice : notices) {
                                if (MapUtils.isNotEmpty(notice)) {
                                    final Object vid = notice.get("vid");
                                    final String json = JSON_UTILS.toJson(notice);
                                    kafkaStreamVehicleNoticeSender.emit((String) vid, json);
                                }
                            }
                        }

                    }
                }
            }

            // endregion

            // region 每5分钟执行一次活跃数据扫描，将闲置车辆告警发到kafka中

            class TimeOutClass implements Runnable {

                @Override
                public void run() {
                    try {

                        try {
//                            ParamsRedis.rebulid();
                            /**
                             * 重新初始化 配置参数，里程跳变数字、未定位的 判断次数等
                             * 由于此方法内部已经调用了 ParamsRedis.rebulid()
                             * 因此可以省略 ParamsRedis 重新初始化方法
                             */
                            CarRuleHandler.rebulid();
                            //从配置文件中读出超时时间
                            Object idleTimeoutMillisecond = ParamsRedisUtil.getInstance().PARAMS.get(ParamsRedisUtil.VEHICLE_IDLE_TIMEOUT_MILLISECOND);
                            if (null != idleTimeoutMillisecond) {
                                CarNoticeBolt.this.idleTimeoutMillisecond = (int) idleTimeoutMillisecond;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        // 车辆长期离线（闲置车辆）通知
                        // 这里容易出现的问题是, 判断线程和数据刷新线程没有做到线程安全, 在判断逻辑执行过程中, 状态是不断刷新的, 容易引起线程冲突或者上下文不一致等诡异的问题.
                        final List<Map<String, Object>> msgs = carOnOffhandler.fullDoesNotice("TIMEOUT", ScanRange.AliveData, System.currentTimeMillis(), CarNoticeBolt.this.idleTimeoutMillisecond);
                        if (CollectionUtils.isNotEmpty(msgs)) {
                            for (Map<String, Object> map : msgs) {
                                if (MapUtils.isNotEmpty(map)) {
                                    Object vid = map.get("vid");
                                    String json = JSON_UTILS.toJson(map);
                                    kafkaStreamVehicleNoticeSender.emit((String) vid, json);
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
            // 每5分钟执行一次
            Executors
                .newScheduledThreadPool(1)
                .scheduleAtFixedRate(
                    new TimeOutClass(),
                    0,
                    60 * 5,
                    TimeUnit.SECONDS);
            // endregion

        } catch (Exception e) {
            LOG.error("初始化闲置车辆判断异常", e);
        }
    }

    @Override
    public void execute(
        @NotNull final Tuple input) {

        dataStreamReceiver.execute(input);
    }

    @SuppressWarnings("AlibabaMethodTooLong")
    private void executeFromDataStream(
        @NotNull final Tuple input,
        @NotNull final String vid,
        @NotNull final ImmutableMap<String, String> data) {

        collector.ack(input);

        final long currentTimeMillis = System.currentTimeMillis();

        // region 离线判断: 如果时间差大于离线检查时间，则进行离线检查, 如果车辆离线，则发送此车辆的所有故障码结束通知
        if (currentTimeMillis - lastOfflineCheckTimeMillisecond >= offlineCheckSpanMillisecond) {

            lastOfflineCheckTimeMillisecond = currentTimeMillis;
            List<Map<String, Object>> msgs = faultCodeHandler.generateNotice(currentTimeMillis);

            if (null != msgs && msgs.size() > 0) {
                for (Map<String, Object> map : msgs) {
                    if (null != map && map.size() > 0) {
                        Object vid2 = map.get("vid");
                        String json = JSON_UTILS.toJson(map);
                        kafkaStreamVehicleNoticeSender.emit((String) vid2, json);
                    }
                }
            }

            //检查所有车辆是否离线，离线则发送离线通知。
            msgs = carRuleHandler.offlineMethod(currentTimeMillis);
            if (null != msgs && msgs.size() > 0) {
                for (Map<String, Object> map : msgs) {
                    if (null != map && map.size() > 0) {
                        Object vid2 = map.get("vid");
                        String json = JSON_UTILS.toJson(map);
                        kafkaStreamVehicleNoticeSender.emit((String) vid2, json);
                    }
                }
            }

            carOnOffhandler.onOffCheck("TIMEOUT", 1, currentTimeMillis, offlineTimeMillisecond);
        }
        // endregion

        if (MapUtils.isEmpty(data)) {
            return;
        }

        PARAMS_REDIS_UTIL.autoLog(vid, () -> LOG.warn("VID[{}]进入车辆通知处理", vid));

        // region 缓存有效状态

        final String collectTime = data.get(DataKey._2000_TERMINAL_COLLECT_TIME);
        final String serverReceiveTime = data.get(DataKey._9999_PLATFORM_RECEIVE_TIME);
        boolean timeEffective = false;
        if(NumberUtils.isDigits(serverReceiveTime) && NumberUtils.isDigits(collectTime)) {
            try {
                // 误差10分钟内有效
                if (Math.abs(DataUtils.parseFormatTime(serverReceiveTime) - DataUtils.parseFormatTime(collectTime)) <= 1000 * 60 * 10) {
                    timeEffective = true;
                }
            } catch (ParseException e) {
                LOG.debug("时间格式异常", e);
            }

        }

        // region 缓存累计里程有效值
        if(timeEffective) {
            final String totalMileage = data.get(DataKey._2202_TOTAL_MILEAGE);

            PARAMS_REDIS_UTIL.autoLog(vid, () -> LOG.warn("VID[{}][{}][{}]有效累计里程缓存处理", vid, serverReceiveTime, totalMileage));

            if (NumberUtils.isDigits(totalMileage)) {

                try {

                    final ImmutableMap<String, String> usefulTotalMileage =
                        VEHICLE_CACHE.getField(
                            vid,
                            VehicleCache.TOTAL_MILEAGE_FIELD);
                    final String oldTime = usefulTotalMileage.get(VehicleCache.VALUE_TIME_KEY);

                    if (NumberUtils.toLong(oldTime) < NumberUtils.toLong(serverReceiveTime)) {

                        final ImmutableMap<String, String> update = new ImmutableMap.Builder<String, String>()
                            .put(VehicleCache.VALUE_TIME_KEY, serverReceiveTime)
                            .put(VehicleCache.VALUE_DATA_KEY, totalMileage)
                            .build();
                        VEHICLE_CACHE.putField(
                            vid,
                            VehicleCache.TOTAL_MILEAGE_FIELD,
                            update);
                        PARAMS_REDIS_UTIL.autoLog(
                            vid,
                            () -> LOG.info(
                                "VID[{}]更新有效累计里程缓存[{}]",
                                vid,
                                update));
                    } else {
                        PARAMS_REDIS_UTIL.autoLog(
                            vid,
                            () -> LOG.info(
                                "VID[{}]保持有效累计里程值缓存[{}]",
                                vid,
                                usefulTotalMileage));
                    }

                } catch (ExecutionException e) {
                    LOG.warn("获取有效累计里程缓存异常", e);
                }
            } else {
                PARAMS_REDIS_UTIL.autoLog(vid, () -> LOG.warn("无效的累计里程[{}]", totalMileage));
            }
        }
        // endregion

        // region 缓存GPS定位有效值
        if(timeEffective) {
            final String orientationString = data.get(DataKey._2501_ORIENTATION);

            PARAMS_REDIS_UTIL.autoLog(
                vid,
                () -> LOG.warn(
                    "VID[{}][{}][{}]有效定位缓存处理",
                    vid,
                    serverReceiveTime,
                    orientationString));


            if (NumberUtils.isDigits(orientationString)) {

                final int orientationValue = NumberUtils.toInt(orientationString);
                if (DataUtils.isOrientationUseful(orientationValue)) {

                    final String longitudeString = data.get(DataKey._2502_LONGITUDE);
                    final String latitudeString = data.get(DataKey._2503_LATITUDE);

                    if(NumberUtils.isDigits(orientationString)
                        && NumberUtils.isDigits(orientationString)) {


                        final int longitudeValue = NumberUtils.toInt(longitudeString);
                        final int latitudeValue = NumberUtils.toInt(latitudeString);

                        if(DataUtils.isOrientationLongitudeUseful(longitudeValue)
                            && DataUtils.isOrientationLatitudeUseful(latitudeValue)) {

                            try {
                                final ImmutableMap<String, String> usefulOrientation =
                                    VEHICLE_CACHE.getField(
                                        vid,
                                        VehicleCache.ORIENTATION_FIELD);
                                final String oldOrientationTime = usefulOrientation.get(VehicleCache.VALUE_TIME_KEY);

                                final ImmutableMap<String, String> usefulLongitude =
                                    VEHICLE_CACHE.getField(
                                        vid,
                                        VehicleCache.LONGITUDE_FIELD);
                                final String oldLongitudeTime = usefulLongitude.get(VehicleCache.VALUE_TIME_KEY);

                                final ImmutableMap<String, String> usefulLatitude =
                                    VEHICLE_CACHE.getField(
                                        vid,
                                        VehicleCache.LATITUDE_FIELD);
                                final String oldLatitudeTime = usefulLatitude.get(VehicleCache.VALUE_TIME_KEY);

                                if (NumberUtils.toLong(oldOrientationTime) < NumberUtils.toLong(serverReceiveTime)) {

                                    final ImmutableMap<String, String> updateOrientation = new ImmutableMap.Builder<String, String>()
                                        .put(VehicleCache.VALUE_TIME_KEY, serverReceiveTime)
                                        .put(VehicleCache.VALUE_DATA_KEY, orientationString)
                                        .build();
                                    VEHICLE_CACHE.putField(
                                        vid,
                                        VehicleCache.ORIENTATION_FIELD,
                                        updateOrientation);
                                    PARAMS_REDIS_UTIL.autoLog(
                                        vid,
                                        () -> LOG.info(
                                            "VID[{}]更新有效定位缓存[{}]",
                                            vid,
                                            updateOrientation));
                                } else {
                                    PARAMS_REDIS_UTIL.autoLog(
                                        vid,
                                        () -> LOG.info(
                                            "VID[{}]保持有效定位缓存[{}]",
                                            vid,
                                            usefulOrientation));
                                }

                                if (NumberUtils.toLong(oldLongitudeTime) < NumberUtils.toLong(serverReceiveTime)) {

                                    final ImmutableMap<String, String> updateLongitude = new ImmutableMap.Builder<String, String>()
                                        .put(VehicleCache.VALUE_TIME_KEY, serverReceiveTime)
                                        .put(VehicleCache.VALUE_DATA_KEY, longitudeString)
                                        .build();
                                    VEHICLE_CACHE.putField(
                                        vid,
                                        VehicleCache.LONGITUDE_FIELD,
                                        updateLongitude);
                                    PARAMS_REDIS_UTIL.autoLog(
                                        vid,
                                        () -> LOG.info(
                                            "VID[{}]更新有效经度缓存[{}]",
                                            vid,
                                            updateLongitude));
                                } else {
                                    PARAMS_REDIS_UTIL.autoLog(
                                        vid,
                                        () -> LOG.info(
                                            "VID[{}]保持有效经度缓存[{}]",
                                            vid,
                                            usefulLongitude));
                                }

                                if (NumberUtils.toLong(oldLatitudeTime) < NumberUtils.toLong(serverReceiveTime)) {

                                    final ImmutableMap<String, String> updateLatitude = new ImmutableMap.Builder<String, String>()
                                        .put(VehicleCache.VALUE_TIME_KEY, serverReceiveTime)
                                        .put(VehicleCache.VALUE_DATA_KEY, latitudeString)
                                        .build();
                                    VEHICLE_CACHE.putField(
                                        vid,
                                        VehicleCache.LATITUDE_FIELD,
                                        updateLatitude);
                                    PARAMS_REDIS_UTIL.autoLog(
                                        vid,
                                        () -> LOG.info(
                                            "VID[{}]更新有效纬度缓存[{}]",
                                            vid,
                                            updateLatitude));
                                } else {
                                    PARAMS_REDIS_UTIL.autoLog(
                                        vid,
                                        () -> LOG.info(
                                            "VID[{}]保持有效纬度缓存[{}]",
                                            vid,
                                            usefulLatitude));
                                }
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            }
                        } else {
                            PARAMS_REDIS_UTIL.autoLog(
                                vid,
                                () -> LOG.warn(
                                    "VID[{}]经纬度超出范围[{}, {}]",
                                    vid,
                                    longitudeString,
                                    latitudeValue));
                        }
                    } else {
                        PARAMS_REDIS_UTIL.autoLog(
                            vid,
                            () -> LOG.warn(
                                "VID[{}]经纬度格式错误[{}][{}]",
                                vid,
                                longitudeString,
                                latitudeString));
                    }
                } else {
                    PARAMS_REDIS_UTIL.autoLog(
                        vid,
                        () -> LOG.warn(
                            "VID[{}]定位无效[{}]",
                            vid,
                            orientationString));
                }
            } else {
                PARAMS_REDIS_UTIL.autoLog(
                    vid,
                    () -> LOG.warn(
                        "VID[{}]定位状态格式错误[{}]",
                        vid,
                        orientationString));
            }
        }
        // endregion

        // endregion 缓存有效状态

        // region 更新实时缓存
        try {
            final String type = data.get(DataKey.MESSAGE_TYPE);
            if (!CommandType.SUBMIT_LINKSTATUS.equals(type)) {
                SysRealDataCache.updateCache(data, currentTimeMillis, idleTimeoutMillisecond);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // endregion

        //返回车辆通知,(重要)
        //先检查规则是否启用，启用了，则把dat放到相应的处理方法中。将返回结果放到list中，返回。
        List<Map<String, Object>> msgs = carRuleHandler.generateNotices(data);
        for (Map<String, Object> map : msgs) {
            if (null != map && map.size() > 0) {
                String json = JSON_UTILS.toJson(map);
                kafkaStreamVehicleNoticeSender.emit(vid, json);
            }
        }

        final List<Map<String, Object>> faultCodeMessages = faultCodeHandler.generateNotice(data);
        if (CollectionUtils.isNotEmpty(faultCodeMessages)) {
            for (Map<String, Object> map : faultCodeMessages) {
                if (null != map && map.size() > 0) {
                    String json = JSON_UTILS.toJson(map);
                    kafkaStreamVehicleNoticeSender.emit(vid, json);
                }
            }
        }
        //如果下线了，则发送上下线的里程值
        Map<String, Object> map = carOnOffhandler.generateNotices(data, currentTimeMillis, offlineTimeMillisecond);
        if (null != map && map.size() > 0) {
            String json = JSON_UTILS.toJson(map);
            kafkaStreamVehicleNoticeSender.emit(vid, json);
        }
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        KAFKA_STREAM.declareOutputFields(KAFKA_STREAM_ID, declarer);
    }

}
