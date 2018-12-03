package storm.stream;

import com.google.common.collect.ImmutableMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.StreamFieldKey;

import java.io.Serializable;

/**
 * @author: xzp
 * @date: 2018-12-03
 * @description:
 */
public final class DataCacheStream implements IStreamFields, Serializable {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DataCacheStream.class);

    @NotNull
    private static final DataCacheStream SINGLETON = new DataCacheStream();

    @Contract(pure = true)
    public static DataCacheStream getInstance() {
        return SINGLETON;
    }

    private DataCacheStream() {
    }

    @NotNull
    private static final Fields FIELDS = new Fields(StreamFieldKey.VEHICLE_ID, StreamFieldKey.DATA, StreamFieldKey.CACHE);

    @Override
    @NotNull
    @Contract(pure = true)
    public final Fields getFields() {
        return FIELDS;
    }

    @Override
    @NotNull
    public final String getStreamId(
        @NotNull final String componentId) {

        return new StringBuilder(64)
            .append(componentId)
            .append('-')
            .append(DataCacheStream.class.getSimpleName())
            .toString();
    }

    @NotNull
    public void declareOutputFields(
        @NotNull final String streamId,
        @NotNull final OutputFieldsDeclarer declarer) {

        declarer.declareStream(streamId, FIELDS);
    }

    @Contract("_, _ -> new")
    @NotNull
    public DataCacheStream.BoltSender prepareSender(
        @NotNull final String streamId,
        @NotNull final OutputCollector collector) {

        return new DataCacheStream.BoltSender(streamId, collector);
    }

    @Contract("_ -> new")
    @NotNull
    public IStreamReceiver prepareReceiver(
        @NotNull final DataCacheStream.IProcessor processor) {

        return new DataCacheStream.Receiver(processor);
    }

    public static class BoltSender {

        @NotNull
        private final String streamId;

        @NotNull
        private final OutputCollector collector;

        public BoltSender(
            @NotNull final String streamId,
            @NotNull final OutputCollector collector) {

            this.streamId = streamId;
            this.collector = collector;
        }

        public void emit(
            @NotNull final Tuple anchors,
            @NotNull final String vehicleId,
            @NotNull final ImmutableMap<String, String> data,
            @NotNull final ImmutableMap<String, String> cache) {

            collector.emit(streamId, anchors, new Values(vehicleId, data, cache));
        }
    }

    private static class Receiver implements IStreamReceiver, Serializable {

        private static final long serialVersionUID = -1534115171552343866L;

        private final DataCacheStream.IProcessor processor;

        public Receiver(
            @NotNull final DataCacheStream.IProcessor processor) {

            this.processor = processor;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void execute(
            final @NotNull Tuple input) {

            final String vehicleId = input.getStringByField(StreamFieldKey.VEHICLE_ID);
            final ImmutableMap<String, String> data =
                ((ImmutableMap<String, String>) input.getValueByField(StreamFieldKey.DATA));
            final ImmutableMap<String, String> cache =
                ((ImmutableMap<String, String>) input.getValueByField(StreamFieldKey.CACHE));

            processor.execute(input, vehicleId, data, cache);
        }
    }

    @FunctionalInterface
    public interface IProcessor {

        /**
         * 处理元组
         * @param input 输入元组
         * @param vehicleId 车辆标识
         * @param data 数据字典
         * @param cache 缓存字典
         */
        void execute(
            @NotNull final Tuple input,
            @NotNull final String vehicleId,
            @NotNull final ImmutableMap<String, String> data,
            @NotNull final ImmutableMap<String, String> cache);
    }
}
