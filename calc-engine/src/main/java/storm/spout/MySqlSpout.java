package storm.spout;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.ObjectExtension;
import storm.stream.MessageId;
import storm.stream.StreamReceiverFilter;
import storm.stream.VehicleIdentityStream;
import storm.util.ConfigUtils;
import storm.util.SqlUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-10-22
 * @description:
 * 1. 从数据库查询有效的车辆标识
 * 2. 将车辆标识分发到下游, 用于下游组件根据车辆标识在正确的组件中加载状态
 */
public final class MySqlSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSpout.class);

    private static final SqlUtils SQL_UTILS = SqlUtils.getInstance();

    // region Component

    @NotNull
    private static final String COMPONENT_ID = MySqlSpout.class.getSimpleName();

    @SuppressWarnings("unused")
    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    // region VehicleIdentityStream

    @NotNull
    private static final VehicleIdentityStream VEHICLE_IDENTITY_STREAM = VehicleIdentityStream.getInstance();

    @NotNull
    private static final String VEHICLE_IDENTITY_STREAM_ID = VEHICLE_IDENTITY_STREAM.getStreamId(COMPONENT_ID);

    @SuppressWarnings("unused")
    @NotNull
    @Contract(pure = true)
    public static String getVehicleIdentityStreamId() {
        return VEHICLE_IDENTITY_STREAM_ID;
    }

    @SuppressWarnings("unused")
    @NotNull
    public static StreamReceiverFilter prepareVehicleIdentityStreamReceiver(
        @NotNull final VehicleIdentityStream.IProcessor processor) {

        return VEHICLE_IDENTITY_STREAM
            .prepareReceiver(
                processor)
            .filter(
                COMPONENT_ID,
                VEHICLE_IDENTITY_STREAM_ID);
    }

    // endregion VehicleIdentityStream

    // region 数据库查询

    /**
     * 最近一次从数据库更新的时间
     */
    private static long lastRebuildTime = 0;

    /**
     * 车辆标识
     */
    private static ImmutableSet<String> vehicleIdSet = ImmutableSet.of();

    private static ImmutableSet<String> getVehicleIdSet() {
        final long currentTimeMillis = System.currentTimeMillis();
		long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushTime());
        if(currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {
            rebuild(currentTimeMillis);
        }

        return vehicleIdSet;
    }

    private static synchronized void rebuild(final long currentTimeMillis) {
		long DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND = TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushTime());
        if(currentTimeMillis - lastRebuildTime > DB_CACHE_FLUSH_MIN_TIME_SPAN_MILLISECOND) {

            try {

                LOG.info("车辆标识重构开始.");

                vehicleIdSet = buildVehicleIdFromDb();

                LOG.info("车辆标识重构完毕.");

                lastRebuildTime = currentTimeMillis;
            } catch (final Exception e) {
                LOG.warn("车辆标识重构异常", e);
            }

        }
    }

    @NotNull
    private static synchronized ImmutableSet<String> buildVehicleIdFromDb() {
        return ObjectExtension.defaultIfNull(
            SQL_UTILS.query(
                    ConfigUtils.getSysParam().getVehicleIdSql(),
                resultSet -> {
                    ImmutableSet.Builder<String> builder = new ImmutableSet.Builder<>();
                    while (resultSet.next()) {
                        final String vehicleId = resultSet.getString(1);
                        builder.add(vehicleId);
                    }
                    return builder.build();
                }
            ),
            ImmutableSet::of);
    }

    // endregion

    private static final long SUCCESSFUL_POLL_INTERVAL_IN_MILLISECONDS = TimeUnit.MINUTES.toMillis(3);

    static {
        LOG.info("车辆标识数据库查询语句为 {} ", ConfigUtils.getSysParam().getVehicleIdSql());
        LOG.info("车辆标识数据库更新最小间隔为 {} 毫秒", TimeUnit.SECONDS.toMillis(ConfigUtils.getSysDefine().getDbCacheFlushTime()));
    }

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private transient SpoutOutputCollector collector;

    private transient VehicleIdentityStream.SpoutSender vehicleIdentityStreamSender;

    @Override
    public void open(
        @NotNull final Map conf,
        @NotNull final TopologyContext context,
        @NotNull final SpoutOutputCollector collector) {

        this.collector = collector;

        vehicleIdentityStreamSender = VEHICLE_IDENTITY_STREAM.openSender(VEHICLE_IDENTITY_STREAM_ID, collector);
    }

    @Override
    public void nextTuple() {

        final ImmutableSet<String> vehicleIdSet = getVehicleIdSet();

        if (CollectionUtils.isNotEmpty(vehicleIdSet)) {
            vehicleIdSet.forEach(vehicleId ->
                vehicleIdentityStreamSender.emit(
                    new MessageId<>(vehicleId),
                    vehicleId
                )
            );
        }

        Utils.sleep(SUCCESSFUL_POLL_INTERVAL_IN_MILLISECONDS);
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        VEHICLE_IDENTITY_STREAM.declareOutputFields(VEHICLE_IDENTITY_STREAM_ID, declarer);
    }
}
