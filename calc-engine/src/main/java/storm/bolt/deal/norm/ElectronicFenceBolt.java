package storm.bolt.deal.norm;

import com.google.common.collect.ImmutableMap;
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
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.event.Event;
import storm.protocol.CommandType;
import storm.stream.KafkaStream;
import storm.stream.StreamReceiverFilter;
import storm.system.DataKey;
import storm.system.SysDefine;
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

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        prepareStreamSender(stormConf, collector);

        prepareStreamReceiver();
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

        try {

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

            // region 当前定位

            final String currentOrientationString= data.get(DataKey._2501_ORIENTATION);
            if(!NumberUtils.isDigits(currentOrientationString)) {
                return;
            }
            final int currentOrientation = NumberUtils.toInt(currentOrientationString);
            if(!DataUtils.isOrientationUseful(currentOrientation)) {
                return;
            }
            final String currentLongitudeString = data.get(DataKey._2502_LONGITUDE);
            if(!NumberUtils.isDigits(currentLongitudeString)) {
                return;
            }
            final double currentLongitude = NumberUtils.toInt(currentOrientationString) / DataKey.ORIENTATION_PRECISION;
            final String currentLatitudeString = data.get(DataKey._2503_LATITUDE);
            if(!NumberUtils.isDigits(currentLatitudeString)) {
                return;
            }
            final double currentLatitude = NumberUtils.toInt(currentOrientationString) / DataKey.ORIENTATION_PRECISION;

            // endregion 当前定位

            // region 缓存定位

            final String cacheOrientationString= cache.get(DataKey._2501_ORIENTATION);
            if(!NumberUtils.isDigits(cacheOrientationString)) {
                return;
            }
            final int cacheOrientation = NumberUtils.toInt(currentOrientationString);
            final String cacheLongitudeString = cache.get(DataKey._2502_LONGITUDE);
            if(!NumberUtils.isDigits(cacheLongitudeString)) {
                return;
            }
            if(!DataUtils.isOrientationUseful(cacheOrientation)) {
                return;
            }
            final double cacheLongitude = NumberUtils.toInt(currentOrientationString) / DataKey.ORIENTATION_PRECISION;
            final String cacheLatitudeString = cache.get(DataKey._2503_LATITUDE);
            if(!NumberUtils.isDigits(cacheLatitudeString)) {
                return;
            }
            final double cacheLatitude = NumberUtils.toInt(currentOrientationString) / DataKey.ORIENTATION_PRECISION;

            // endregion 缓存定位

            if (!isLocationEffective(currentLongitude, currentLatitude, cacheLongitude, cacheLatitude)) {
                return;
            }

            final Coordinate coordinate = new Coordinate(currentLongitude, currentLatitude);
            // TODO 徐志鹏: 加到配置文件中
            final double distance = 50;
            getVehicleFences(vehicleId)
                .values()
                .forEach(fence -> fence.process(
                    coordinate,
                    distance,
                    platformReceiveTime,
                    (f, e) -> insideCallback(f, e, data, cache),
                    (f, e) -> outsideCallback(f, e, data, cache))
                );

        } catch (@NotNull final Exception e) {
            LOG.warn("电子围栏计算异常, VID[{}]", vehicleId, e);
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

    @Contract(pure = true)
    private boolean isLocationEffective(
        final double currentLongitude,
        final double currentLatitude,
        final double cacheLongitude,
        final double cacheLatitude) {

        final double longitude = currentLongitude - cacheLongitude;
        final double latitude = currentLatitude - cacheLatitude;

        // TODO 徐志鹏: 加到配置文件中
        final double maxEffectiveDistance = 1000;

        return longitude * longitude + latitude * latitude <= maxEffectiveDistance * maxEffectiveDistance;
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
