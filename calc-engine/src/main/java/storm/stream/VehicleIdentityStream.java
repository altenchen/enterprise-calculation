package storm.stream;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.StreamFieldKey;

/**
 * @author: xzp
 * @date: 2018-09-07
 * @description: 车辆标识流
 */
public final class VehicleIdentityStream implements IStreamFields {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(VehicleIdentityStream.class);

    @NotNull
    private static final VehicleIdentityStream SINGLETON = new VehicleIdentityStream();

    @Contract(pure = true)
    public static VehicleIdentityStream getInstance() {
        return SINGLETON;
    }

    private VehicleIdentityStream() {
    }

    @NotNull
    private static final Fields FIELDS = new Fields(StreamFieldKey.VEHICLE_ID);

    @Contract(pure = true)
    @Override
    public @NotNull Fields getFields() {
        return FIELDS;
    }

    @Override
    @NotNull
    public String getStreamId(
        @NotNull final String componentId) {

        return new StringBuilder(64)
            .append(componentId)
            .append('-')
            .append(VehicleIdentityStream.class.getSimpleName())
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
    public SpoutSender openSender(
        @NotNull final String streamId,
        @NotNull final SpoutOutputCollector collector) {

        return new SpoutSender(streamId, collector);
    }

    @Contract("_, _ -> new")
    @NotNull
    public BoltSender prepareSender(
        @NotNull final String streamId,
        @NotNull final OutputCollector collector) {

        return new BoltSender(streamId, collector);
    }

    @Contract("_ -> new")
    @NotNull
    public IStreamReceiver prepareReceiver(
        @NotNull final IProcessor processor) {

        return new Receiver(processor);
    }


    public static class SpoutSender {

        @NotNull
        private final String streamId;

        @NotNull
        private final SpoutOutputCollector collector;

        public SpoutSender(
            @NotNull final String streamId,
            @NotNull final SpoutOutputCollector collector) {

            this.streamId = streamId;
            this.collector = collector;
        }

        public void emit(
            @NotNull final String vid) {

            collector.emit(streamId, new Values(vid));
        }

        public <T> void emit(
            @NotNull final MessageId<T> messageId,
            @NotNull final String vid) {

            collector.emit(streamId, new Values(vid), messageId);
        }
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
            @NotNull final String vid) {

            collector.emit(streamId, anchors, new Values(vid));
        }
    }

    private static class Receiver implements IStreamReceiver {

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

            processor.execute(input, vid);
        }
    }

    @FunctionalInterface
    public interface IProcessor {

        /**
         * 处理元组
         *
         * @param input 输入元组
         * @param vid   车辆标识
         */
        void execute(
            @NotNull final Tuple input,
            @NotNull final String vid);
    }
}
