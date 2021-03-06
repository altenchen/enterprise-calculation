package storm.stream;

import org.apache.storm.kafka.spout.KafkaTuple;
import org.apache.storm.spout.SpoutOutputCollector;
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
 * @description: kafka 车辆实时消息流
 */
public final class GeneralStream implements IStreamFields, Serializable {

    private static final long serialVersionUID = 8984099418783635935L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(GeneralStream.class);

    @NotNull
    private static final GeneralStream SINGLETON = new GeneralStream();

    @Contract(pure = true)
    public static GeneralStream getInstance() {
        return SINGLETON;
    }

    private GeneralStream() {
    }

    @NotNull
    private static final Fields FIELDS = new Fields(StreamFieldKey.VEHICLE_ID, StreamFieldKey.KAFKA_MESSAGE);

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
            .append(GeneralStream.class.getSimpleName())
            .toString();
    }

    public void declareOutputFields(
        @NotNull final String streamId,
        @NotNull final OutputFieldsDeclarer declarer) {

        declarer.declareStream(streamId, FIELDS);
    }

    @Contract("_ -> new")
    @NotNull
    public GeneralStream.KafkaSender declareOutputFields(
        @NotNull final String streamId) {

        return new KafkaSender(FIELDS, streamId);
    }

    @Contract("_, _ -> new")
    @NotNull
    public GeneralStream.SpoutSender prepareSpoutSender(
        @NotNull final String streamId,
        @NotNull final SpoutOutputCollector collector) {

        return new SpoutSender(streamId, collector);
    }

    @Contract("_ -> new")
    @NotNull
    public IStreamReceiver prepareReceiver(
        @NotNull final IProcessor processor) {

        return new Receiver(processor);
    }

    public static class KafkaSender implements Serializable {

        private static final long serialVersionUID = 5430182532897405011L;

        @NotNull
        private final Fields fields;

        @NotNull
        private final String streamId;

        public KafkaSender(
            @NotNull final Fields fields,
            @NotNull final String streamId) {

            this.fields = fields;
            this.streamId = streamId;
        }

        @NotNull
        public Fields getFields() {
            return fields;
        }

        @NotNull
        public String getStreamId() {
            return streamId;
        }

        public KafkaTuple emit(
            @NotNull final String vid,
            @NotNull final String message) {

            return new KafkaTuple(vid, message).routedTo(streamId);
        }
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
            @NotNull final String vid,
            @NotNull final String message) {

            collector.emit(streamId, new Values(vid, message));
        }

        public <T> void emit(
            @NotNull final MessageId<T> messageId,
            @NotNull final String vid,
            @NotNull final String message) {

            collector.emit(streamId, new Values(vid, message), messageId);
        }
    }

    private static class Receiver implements IStreamReceiver, Serializable {

        private static final long serialVersionUID = 7665095012556962886L;

        private final IProcessor processor;

        Receiver(
            @NotNull final IProcessor processor) {

            this.processor = processor;
        }

        @Override
        public void execute(
            final @NotNull Tuple input) {

            final String vid = input.getStringByField(StreamFieldKey.VEHICLE_ID);
            final String message = input.getStringByField(StreamFieldKey.KAFKA_MESSAGE);

            processor.execute(input, vid, message);
        }
    }

    @FunctionalInterface
    public interface IProcessor {

        /**
         * 处理元组
         * @param input 输入元组
         * @param vid 车辆标识
         * @param message kafka 消息
         */
        void execute(
            @NotNull final Tuple input,
            @NotNull final String vid,
            @NotNull final String message);
    }
}
