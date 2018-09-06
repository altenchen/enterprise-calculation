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
 * @date: 2018-09-05
 * @description: 车辆实时数据流
 */
public class DataStream implements IStreamFields, Serializable {

    private static final long serialVersionUID = 8672249921073108932L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DataStream.class);

    @NotNull
    private static final DataStream SINGLETON = new DataStream();

    @Contract(pure = true)
    public static DataStream getInstance() {
        return SINGLETON;
    }

    private DataStream() {
    }

    @NotNull
    private static final Fields FIELDS = new Fields(StreamFieldKey.VEHICLE_ID, StreamFieldKey.DATA);

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
            .append(DataStream.class.getSimpleName())
            .toString();
    }

    @NotNull
    public void declareOutputFields(
        @NotNull final String streamId,
        @NotNull final OutputFieldsDeclarer declarer) {

        declarer.declareStream(streamId, FIELDS);
    }

    @NotNull
    public Sender prepareSender(
        @NotNull final String streamId,
        @NotNull final OutputCollector collector) {

        return new Sender(streamId, collector);
    }

    @Contract("_ -> new")
    @NotNull
    public IStreamReceiver prepareReceiver(
        @NotNull final IProcessor processor) {

        return new DataStream.Receiver(processor);
    }

    public static class Sender implements Serializable {

        private static final long serialVersionUID = 3540867678579637519L;

        @NotNull
        private final String streamId;

        @NotNull
        private final OutputCollector collector;

        public Sender(
            @NotNull final String streamId,
            @NotNull final OutputCollector collector) {

            this.streamId = streamId;
            this.collector = collector;
        }

        public void emit(
            @NotNull final Tuple anchors,
            @NotNull final String vid,
            @NotNull final ImmutableMap<String, String> data) {

            collector.emit(streamId, anchors, new Values(vid, data));
        }
    }

    private static class Receiver implements IStreamReceiver, Serializable {

        private static final long serialVersionUID = -1534115171552343866L;

        private final IProcessor processor;

        public Receiver(
            @NotNull final IProcessor processor) {

            this.processor = processor;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void execute(
            final @NotNull Tuple input) {

            final String vid = input.getStringByField(StreamFieldKey.VEHICLE_ID);
            final ImmutableMap<String, String> data =
                ((ImmutableMap<String, String>) input.getValueByField(StreamFieldKey.DATA));

            processor.execute(input, vid, data);
        }
    }

    @FunctionalInterface
    public interface IProcessor {

        /**
         * 处理元组
         * @param input 输入元组
         * @param vid 车辆标识
         * @param data 数据字典
         */
        void execute(
            @NotNull final Tuple input,
            @NotNull final String vid,
            @NotNull final ImmutableMap<String, String> data);
    }
}
