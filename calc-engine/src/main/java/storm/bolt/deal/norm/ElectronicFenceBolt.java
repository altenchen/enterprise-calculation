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
import storm.dao.DataToRedis;
import storm.domain.fence.Fence;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.notice.BaseNotice;
import storm.domain.fence.service.IFenceQueryService;
import storm.domain.fence.service.impl.FenceQueryMysqlServiceImpl;
import storm.domain.fence.status.FenceVehicleStatus;
import storm.protocol.CommandType;
import storm.stream.KafkaStream;
import storm.stream.StreamReceiverFilter;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.DataUtils;
import storm.util.JsonUtils;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-11-26
 * @description:
 */
public final class ElectronicFenceBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ElectronicFenceBolt.class);

    @NotNull
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    /**
     * 经纬度坐标系中距离为1的两个坐标间近似距离93164米, 这里简化为1000000
     */
    private static final long COORDINATE_COEFFICIENT = ConfigUtils.getSysDefine().getFenceCoordinateCoefficient();

    @NotNull
    private static final IFenceQueryService fenceQueryService = new FenceQueryMysqlServiceImpl();

    @NotNull
    private static final DataToRedis DATA_TO_REDIS = new DataToRedis();

    // region Component

    @NotNull
    private static final String COMPONENT_ID = ElectronicFenceBolt.class.getSimpleName();

    @SuppressWarnings("unused")
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

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private transient OutputCollector collector;

    private transient StreamReceiverFilter dataCacheStreamReceiver;

    private transient KafkaStream.Sender kafkaStreamFenceAlarmSender;

    /**
     * <fenceId, <vehicleId, status>>
     */
    private transient Map<String, Map<String, FenceVehicleStatus>> fenceVehicleStatus;

    @NotNull
    private FenceVehicleStatus ensureStatus(
        @NotNull final String fenceId,
        @NotNull final String vehicleId
    ) {
        return fenceVehicleStatus
            .computeIfAbsent(
                fenceId,
                fid -> Maps.newHashMap())
            .computeIfAbsent(
                vehicleId,
                vid -> new FenceVehicleStatus(fenceId, vehicleId, DATA_TO_REDIS));
    }

    private transient long lastCleanStatusTime;

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        prepareStreamSender(stormConf, collector);

        prepareStreamReceiver();

        fenceVehicleStatus = Maps.newHashMap();
    }

    private void prepareStreamSender(
        @NotNull final Map stormConf,
        @NotNull final OutputCollector collector) {

        final KafkaStream.SenderBuilder kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        final String fenceAlarmTopic = Objects.toString(stormConf.get(SysDefine.KAFKA_PRODUCER_VEHICLE_FENCE_ALARM_TOPIC), null);
        if (StringUtils.isBlank(fenceAlarmTopic)) {
            LOG.error("电子围栏kafka输出主题为空, 请配置[{}]", SysDefine.KAFKA_PRODUCER_VEHICLE_FENCE_ALARM_TOPIC);
        }
        kafkaStreamFenceAlarmSender = kafkaStreamSenderBuilder.build(fenceAlarmTopic);
    }

    private void prepareStreamReceiver() {

        dataCacheStreamReceiver = FilterBolt.prepareDataCacheStreamReceiver(this::executeFromDataCacheStream);
    }

    @Override
    public void execute(
        @NotNull final Tuple input) {

        if (TupleUtils.isTick(input)) {
            executeFromSystemTickStream(input);
            return;
        }

        if (dataCacheStreamReceiver.execute(input)) {
            return;
        }

        collector.fail(input);
    }

    private void executeFromSystemTickStream(
        @NotNull final Tuple input) {

        collector.ack(input);

        final long currentTimeMillis = System.currentTimeMillis();

        // 每分钟清理一次不可用的电子围栏未结束事件通知
        if (currentTimeMillis - lastCleanStatusTime > TimeUnit.MINUTES.toMillis(1)) {
            lastCleanStatusTime = currentTimeMillis;
            cleanStatus(input);
        }
    }

    private void cleanStatus(
        @NotNull final Tuple input) {

        long time = System.currentTimeMillis();
        //清理内存中的状态
        fenceVehicleStatus.entrySet().removeIf(fenceEntry -> {

            String fenceId = fenceEntry.getKey();
            boolean existFence = existFence(fenceId, time);

            fenceEntry.getValue().entrySet().removeIf(vehicleEntry -> {

                String vehicleId = vehicleEntry.getKey();

                FenceVehicleStatus status = vehicleEntry.getValue();

                if (!existFence) {
                    Set<String> eventIds = new HashSet<>();
                    //围栏区域已修改或启用时间等已被修改导致处理激活外的，从redis删除车辆驶入驶出状态
                    status.cleanStatus(
                        (_fenceId, eventId) -> {
                            eventIds.add(_fenceId + "." + eventId);
                            return false;
                        },
                        notice -> emitToKafka(input, notice)
                    );
                    fenceQueryService.deleteFenceVehicleStatusCache(fenceId, vehicleId, eventIds);
                } else {
                    status.cleanStatus(
                        // 清理无效的事件
                        this::existFenceEvent,
                        notice -> emitToKafka(input, notice)
                    );
                }

                // 清理无效的车辆
                return !existFenceVehicle(fenceId, vehicleId);
            });
            // 清理无效的围栏
            return !existFence;
        });

        /**
         * 脏数据检查
         */
        fenceQueryService.dataCheck((vid, notice) -> emitToKafka(input, vid, notice));
    }

    private void executeFromDataCacheStream(
        @NotNull final Tuple input,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache) {

        collector.ack(input);

        final String messageType = data.get(DataKey.MESSAGE_TYPE);
        if (StringUtils.isBlank(messageType)) {
            return;
        }

        if (!CommandType.SUBMIT_REALTIME.equals(messageType)) {
            return;
        }

        final double maxDistance = ConfigUtils.getSysDefine().getFenceCoordinateDistanceMaxMeter() / COORDINATE_COEFFICIENT;
        final double insideDistance = ConfigUtils.getSysDefine().getFenceShapeBufferInsideMeter() / COORDINATE_COEFFICIENT;
        final double outsideDistance = ConfigUtils.getSysDefine().getFenceShapeBufferOutsideMeter() / COORDINATE_COEFFICIENT;

        try {
            Optional.ofNullable(parsePlatformReceiveTime(vehicleId, data, cache))
                // 具备有效时间
                .ifPresent(platformReceiveTime ->
                    Optional.ofNullable(parseCoordinate(vehicleId, data, cache, maxDistance))
                        // 具备有效定位
                        .ifPresent(coordinate ->
                            getVehicleFences(vehicleId)
                                .values()
                                .stream()
                                .filter(fence -> fence.active(platformReceiveTime))
                                // 逐个处理激活的电子围栏
                                .forEach(fence -> fence.process(
                                    coordinate,
                                    insideDistance,
                                    outsideDistance,
                                    platformReceiveTime,
                                    (whichSide, activeEventMap) ->
                                        // 逐个处理激活的事件
                                        ensureStatus(
                                            fence.getFenceId(),
                                            vehicleId)
                                            .whichSideArea(
                                                platformReceiveTime,
                                                coordinate,
                                                fence,
                                                whichSide,
                                                activeEventMap,
                                                data,
                                                cache,
                                                // 回调: 通知发送到 kafka
                                                notice -> emitToKafka(input, notice)
                                            )
                                    )
                                )
                        )
                );
        } catch (@NotNull final Exception e) {
            LOG.warn("电子围栏计算异常, VID[{}]", vehicleId, e);
        }
    }

    @Nullable
    private Coordinate parseCoordinate(
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        final double maxDistance) {

        // region 当前定位

        final String currentOrientationString = data.get(DataKey._2501_ORIENTATION);
        if (!NumberUtils.isDigits(currentOrientationString)) {
            return null;
        }
        final int currentOrientation = NumberUtils.toInt(currentOrientationString);
        if (!DataUtils.isOrientationUseful(currentOrientation)) {
            return null;
        }
        final String currentLongitudeString = data.get(DataKey._2502_LONGITUDE);
        if (!NumberUtils.isDigits(currentLongitudeString)) {
            return null;
        }
        final double currentLongitude = NumberUtils.toInt(currentLongitudeString) / DataKey.ORIENTATION_PRECISION;
        final String currentLatitudeString = data.get(DataKey._2503_LATITUDE);
        if (!NumberUtils.isDigits(currentLatitudeString)) {
            return null;
        }
        final double currentLatitude = NumberUtils.toInt(currentLatitudeString) / DataKey.ORIENTATION_PRECISION;

        // endregion 当前定位

        final Coordinate coordinate = new Coordinate(currentLongitude, currentLatitude);

        // region 缓存定位

        final String cacheOrientationString = cache.get(DataKey._2501_ORIENTATION);
        if (!NumberUtils.isDigits(cacheOrientationString)) {
            return coordinate;
        }
        final int cacheOrientation = NumberUtils.toInt(cacheOrientationString);
        if (!DataUtils.isOrientationUseful(cacheOrientation)) {
            return coordinate;
        }
        final String cacheLongitudeString = cache.get(DataKey._2502_LONGITUDE);
        if (!NumberUtils.isDigits(cacheLongitudeString)) {
            return coordinate;
        }
        final double cacheLongitude = NumberUtils.toInt(cacheLongitudeString) / DataKey.ORIENTATION_PRECISION;
        final String cacheLatitudeString = cache.get(DataKey._2503_LATITUDE);
        if (!NumberUtils.isDigits(cacheLatitudeString)) {
            return coordinate;
        }
        final double cacheLatitude = NumberUtils.toInt(cacheLatitudeString) / DataKey.ORIENTATION_PRECISION;

        // endregion 缓存定位

        final double longitude = currentLongitude - cacheLongitude;
        final double latitude = currentLatitude - cacheLatitude;

        if (longitude * longitude + latitude * latitude > maxDistance * maxDistance) {
            LOG.warn(
                "VID[{}]两帧数据间定位距离超过{}米, [{},{}]<->[{},{}]",
                vehicleId,
                maxDistance * COORDINATE_COEFFICIENT,
                currentLongitude,
                currentLatitude,
                cacheLongitude,
                cacheLatitude);
            return null;
        } else {
            return coordinate;
        }
    }

    @Nullable
    private Long parsePlatformReceiveTime(
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache) {

        try {
            final long platformReceiveTime = DataUtils.parsePlatformReceiveTime(data);
            if (MapUtils.isNotEmpty(cache)) {
                final long cacheTime = DataUtils.parsePlatformReceiveTime(cache);
                if (platformReceiveTime < cacheTime) {
                    LOG.warn("VID:{} 平台接收时间乱序, {} < {}", vehicleId, platformReceiveTime, cacheTime);
                    return null;
                }
            }
            return platformReceiveTime;
        } catch (final ParseException e) {
            LOG.warn("VID:" + vehicleId + " 解析服务器时间异常", e);
            return null;
        }
    }

    /**
     * 根据车辆ID查询电子围栏列表
     *
     * @param vehicleId 车辆ID
     * @return
     */
    @Contract(pure = true)
    @NotNull
    private ImmutableMap<String, Fence> getVehicleFences(@NotNull final String vehicleId) {
        return fenceQueryService.query(vehicleId);
    }

    /**
     * 判断是否存在有效的电子围栏
     *
     * @param fenceId 电子围栏ID
     * @return
     */
    @Contract(pure = true)
    private boolean existFence(@NotNull final String fenceId, final long time) {
        return fenceQueryService.existFence(fenceId, time);
    }

    /**
     * 判断是否存在有效的电子围栏与车辆关联
     *
     * @param fenceId   电子围栏ID
     * @param vehicleId 车辆ID
     * @return
     */
    @Contract(pure = true)
    private boolean existFenceVehicle(
        @NotNull final String fenceId,
        @NotNull final String vehicleId) {
        return fenceQueryService.existFenceVehicle(fenceId, vehicleId);
    }

    /**
     * 判断是否存在有效的电子围栏与规则关联
     *
     * @param fenceId 电子围栏ID
     * @param eventId 事件(规则)ID
     * @return
     */
    @Contract(pure = true)
    private boolean existFenceEvent(
        @NotNull final String fenceId,
        @NotNull final String eventId) {
        return fenceQueryService.existFenceEvent(fenceId, eventId);
    }

    private void emitToKafka(
        @NotNull final Tuple input,
        @NotNull final BaseNotice notice) {

        final String json = JSON_UTILS.toJson(notice);
        emitToKafka(input, notice.vehicleId, json);
    }

    private void emitToKafka(
        @NotNull final Tuple input,
        @NotNull final String vehicleId,
        @NotNull final String json) {
        kafkaStreamFenceAlarmSender.emit(input, vehicleId, json);
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        KAFKA_STREAM.declareOutputFields(KAFKA_STREAM_ID, declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        final Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return config;
    }
}
