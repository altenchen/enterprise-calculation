package storm.spout;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheKeys;
import com.ctfo.datacenter.cache.handle.CTFOCacheTable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.stream.MessageId;
import storm.stream.StreamReceiverFilter;
import storm.stream.VehicleIdentityStream;
import storm.util.CTFOUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-09-07
 * @description: 单实例, 获取集群中缓存的车辆的标识(vid)
 */
public final class CtfoKeySpout extends BaseRichSpout {

    private static final long serialVersionUID = 9156954870925581218L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CtfoKeySpout.class);

    // region Component

    @NotNull
    private static final String COMPONENT_ID = CtfoKeySpout.class.getSimpleName();

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

    /**
     * 分布式 redis 组合键成员数
     */
    private static final int CLUSTER_KEY_ITEM_COUNT = 3;

    /**
     * 分布式 redis 组合键车辆标识(逻辑键)下标
     */
    private static final int KEY_VID_ITEM_INDEX = 2;

    private static final long SUCCESSFUL_POLL_INTERVAL_IN_MILLISECONDS = TimeUnit.HOURS.toMillis(1);

    private static final long EXCEPTION_POLL_INTERVAL_IN_MILLISECONDS = TimeUnit.SECONDS.toMillis(1);

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

        final CTFOCacheTable ctfoCacheTable = CTFOUtils.getDefaultCTFOCacheTable();
        try {

            final CTFOCacheKeys ctfoCacheKeys = ctfoCacheTable.getCTFOCacheKeys();
            while (ctfoCacheKeys.next()) {

                // 这一批键是在同一个 redis 节点上的
                final List<String> clusterKeys = ctfoCacheKeys.getKeys();
                if(CollectionUtils.isNotEmpty(clusterKeys)) {

                    clusterKeys.forEach(clusterKey->{

                        // xny-realInfo-{vid}
                        final String[] splits = clusterKey.split("-", CLUSTER_KEY_ITEM_COUNT);
                        if(splits.length < CLUSTER_KEY_ITEM_COUNT) {
                            return;
                        }

                        final String vid = splits[KEY_VID_ITEM_INDEX];
                        if (StringUtils.isEmpty(vid)) {
                            return;
                        }

                        vehicleIdentityStreamSender.emit(new MessageId<>(vid), vid);
                    });
                }
            }

            Utils.sleep(SUCCESSFUL_POLL_INTERVAL_IN_MILLISECONDS);
        } catch (DataCenterException e) {
            LOG.warn("获取CTFO实时数据缓存键异常", e);
            Utils.sleep(EXCEPTION_POLL_INTERVAL_IN_MILLISECONDS);
        }
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        VEHICLE_IDENTITY_STREAM.declareOutputFields(VEHICLE_IDENTITY_STREAM_ID, declarer);
    }
}
