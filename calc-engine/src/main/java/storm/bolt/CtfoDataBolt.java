package storm.bolt;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheTable;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.MapUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.spout.CtfoKeySpout;
import storm.stream.DataStream;
import storm.stream.StreamReceiverFilter;
import storm.util.CTFOUtils;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-09-08
 * @description: 获取集群中缓存的车辆的数据
 */
public final class CtfoDataBolt extends BaseRichBolt {

    private static final long serialVersionUID = 2315863729145373726L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CtfoDataBolt.class);

    // region Component

    @NotNull
    private static final String COMPONENT_ID = CtfoDataBolt.class.getSimpleName();

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

    @SuppressWarnings("unused")
    @NotNull
    @Contract(pure = true)
    public static String getDataStreamId() {
        return DATA_STREAM_ID;
    }

    @SuppressWarnings("unused")
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

    @SuppressWarnings("unused")
    private transient OutputCollector collector;

    private transient DataStream.BoltSender dataStreamSender;

    private transient StreamReceiverFilter vehicleIdentityStreamReceiver;

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        dataStreamSender = DATA_STREAM.prepareSender(DATA_STREAM_ID, collector);

        vehicleIdentityStreamReceiver = CtfoKeySpout.prepareVehicleIdentityStreamReceiver(this::executeFromVehicleIdentityStream);
    }

    @Override
    public void execute(
        @NotNull final Tuple input) {

        if (vehicleIdentityStreamReceiver.execute(input)) {
            return;
        }

        collector.fail(input);
    }

    private void executeFromVehicleIdentityStream(
        @NotNull final Tuple input,
        @NotNull final String vid) {

        final CTFOCacheTable ctfoCacheTable = CTFOUtils.getDefaultCTFOCacheTable();

        try {

            final Map<String, String> data = ctfoCacheTable.queryHash(vid);
            if (MapUtils.isEmpty(data)) {
                collector.ack(input);
                return;
            }

            dataStreamSender.emit(input, vid, ImmutableMap.copyOf(data));

            collector.ack(input);
            return;
        } catch (DataCenterException e) {
            LOG.warn("获取CTFO实时数据缓存值异常", e);
        }
        collector.fail(input);
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        DATA_STREAM.declareOutputFields(DATA_STREAM_ID, declarer);
    }
}
