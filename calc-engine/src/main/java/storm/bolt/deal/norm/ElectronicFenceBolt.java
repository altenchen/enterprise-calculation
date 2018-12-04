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
import storm.domain.fence.Fence;
import storm.domain.fence.VehicleStatus;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.event.Event;
import storm.protocol.CommandType;
import storm.stream.KafkaStream;
import storm.stream.StreamReceiverFilter;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.text.ParseException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: xzp
 * @date: 2018-11-26
 * @description:
 */
public final class ElectronicFenceBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ElectronicFenceBolt.class);

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
     * <vehicleId, status>
     */
    private transient Map<String, VehicleStatus> vehicleStatus;

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        prepareStreamSender(stormConf, collector);

        prepareStreamReceiver();

        vehicleStatus = Maps.newHashMap();
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

        if (dataCacheStreamReceiver.execute(input)) {
            return;
        }

        collector.fail(input);
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

        final double maxDistance = ConfigUtils.getSysDefine().getFenceCoordinateDistanceMaxMeter();
        final double insideDistance = ConfigUtils.getSysDefine().getFenceShapeBufferInsideMeter();
        final double outsideDistance = ConfigUtils.getSysDefine().getFenceShapeBufferOutsideMeter();

        try {
            parsePlatformReceiveTime(vehicleId, data, cache)
                .ifPresent(platformReceiveTime ->
                    parseCoordinate(vehicleId, data, cache, maxDistance)
                        .ifPresent(coordinate -> {
                            getVehicleFences(vehicleId)
                                .values()
                                .forEach(fence -> fence.process(
                                    coordinate,
                                    insideDistance,
                                    outsideDistance,
                                    platformReceiveTime,
                                    (f, e) -> insideCallback(f, e, data, cache),
                                    (f, e) -> outsideCallback(f, e, data, cache))
                                );
                        })
                );
        } catch (@NotNull final Exception e) {
            LOG.warn("电子围栏计算异常, VID[{}]", vehicleId, e);
        }
    }

    private Optional<Coordinate> parseCoordinate(
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        final double maxDistance) {

        // region 当前定位

        final String currentOrientationString= data.get(DataKey._2501_ORIENTATION);
        if(!NumberUtils.isDigits(currentOrientationString)) {
            return Optional.empty();
        }
        final int currentOrientation = NumberUtils.toInt(currentOrientationString);
        if(!DataUtils.isOrientationUseful(currentOrientation)) {
            return Optional.empty();
        }
        final String currentLongitudeString = data.get(DataKey._2502_LONGITUDE);
        if(!NumberUtils.isDigits(currentLongitudeString)) {
            return Optional.empty();
        }
        final double currentLongitude = NumberUtils.toInt(currentOrientationString) / DataKey.ORIENTATION_PRECISION;
        final String currentLatitudeString = data.get(DataKey._2503_LATITUDE);
        if(!NumberUtils.isDigits(currentLatitudeString)) {
            return Optional.empty();
        }
        final double currentLatitude = NumberUtils.toInt(currentOrientationString) / DataKey.ORIENTATION_PRECISION;

        // endregion 当前定位

        // region 缓存定位

        final String cacheOrientationString= cache.get(DataKey._2501_ORIENTATION);
        if(!NumberUtils.isDigits(cacheOrientationString)) {
            return Optional.empty();
        }
        final int cacheOrientation = NumberUtils.toInt(currentOrientationString);
        final String cacheLongitudeString = cache.get(DataKey._2502_LONGITUDE);
        if(!NumberUtils.isDigits(cacheLongitudeString)) {
            return Optional.empty();
        }
        if(!DataUtils.isOrientationUseful(cacheOrientation)) {
            return Optional.empty();
        }
        final double cacheLongitude = NumberUtils.toInt(currentOrientationString) / DataKey.ORIENTATION_PRECISION;
        final String cacheLatitudeString = cache.get(DataKey._2503_LATITUDE);
        if(!NumberUtils.isDigits(cacheLatitudeString)) {
            return Optional.empty();
        }
        final double cacheLatitude = NumberUtils.toInt(currentOrientationString) / DataKey.ORIENTATION_PRECISION;

        // endregion 缓存定位

        final double longitude = currentLongitude - cacheLongitude;
        final double latitude = currentLatitude - cacheLatitude;

        if (longitude * longitude + latitude * latitude > maxDistance * maxDistance) {
            LOG.warn(
                "VID[{}]两帧数据间定位距离超过{}米, [{},{}]<->[{},{}]",
                vehicleId,
                maxDistance,
                currentLongitude,
                currentLatitude,
                cacheLongitude,
                cacheLatitude);
            return Optional.empty();
        }

        return Optional.of(new Coordinate(currentLongitude, currentLatitude));
    }

    private Optional<Long> parsePlatformReceiveTime(
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache) {

        try {
            final long platformReceiveTime = DataUtils.parsePlatformReceiveTime(data);
            if (MapUtils.isNotEmpty(cache)) {
                final long cacheTime = DataUtils.parsePlatformReceiveTime(cache);
                if (platformReceiveTime < cacheTime) {
                    LOG.warn("VID:{} 平台接收时间乱序, {} < {}", vehicleId, platformReceiveTime, cacheTime);
                    return Optional.empty();
                }
            }
            return Optional.of(platformReceiveTime);
        } catch (final ParseException e) {
            LOG.warn("VID:" + vehicleId + " 解析服务器时间异常", e);
            return Optional.empty();
        }
    }

    private void insideCallback(
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache) {

        Optional
            .ofNullable(
                event.trigger(
                    data,
                    cache
                )
            )
            .ifPresent(trigger -> {
                if(trigger) {
                    insideTriggerTrue(fence, event, data, cache);
                } else {
                    insideTriggerFalse(fence, event, data, cache);
                }
            });
    }

    private void insideTriggerTrue(
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache) {

        final String fenceId = fence.getFenceId();
        final String eventId = event.getEventId();

        // TODO: 徐志鹏
    }

    private void insideTriggerFalse(
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache) {

        final String fenceId = fence.getFenceId();
        final String eventId = event.getEventId();

        // TODO: 徐志鹏
    }

    private void outsideCallback(
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache) {

        Optional
            .ofNullable(
                event.trigger(
                    data,
                    cache
                )
            )
            .ifPresent(trigger -> {
                if(trigger) {
                    outsideTriggerTrue(fence, event, data, cache);
                } else {
                    outsideTriggerFalse(fence, event, data, cache);
                }
            });
    }

    private void outsideTriggerTrue(
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache) {

        final String fenceId = fence.getFenceId();
        final String eventId = event.getEventId();

        // TODO: 徐志鹏
    }

    private void outsideTriggerFalse(
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache) {

        final String fenceId = fence.getFenceId();
        final String eventId = event.getEventId();

        // TODO: 徐志鹏
    }

    @NotNull
    private ImmutableMap<String, Fence> getVehicleFences(
        @NotNull final String vehicleId
    ) {
        // TODO 许智杰: 替换为从数据库构建出来的缓存
        return ImmutableMap.of();
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        KAFKA_STREAM.declareOutputFields(KAFKA_STREAM_ID, declarer);
    }
}
