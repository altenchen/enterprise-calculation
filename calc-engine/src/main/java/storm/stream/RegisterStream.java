package storm.stream;

import org.apache.storm.kafka.spout.KafkaTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.StreamFieldKey;

import java.io.Serializable;

/**
 * @author: xzp
 * @date: 2018-09-05
 * @description: kafka 车辆注册消息流
 */
public class RegisterStream implements IStreamFields, Serializable {

    private static final long serialVersionUID = 1815930421043329439L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(RegisterStream.class);

    private static final RegisterStream SINGLETON = new RegisterStream();

    @Contract(pure = true)
    public static RegisterStream getInstance() {
        return SINGLETON;
    }

    private RegisterStream() {
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
            .append(RegisterStream.class.getSimpleName())
            .toString();
    }

    @Contract("_ -> new")
    @NotNull
    public Sender declareOutputFields(
        @NotNull final String streamId) {

        return new Sender(FIELDS, streamId);
    }

    @Contract("_ -> new")
    @NotNull
    public IStreamReceiver prepareReceiver(
        @NotNull final IProcessor processor) {

        return new Receiver(processor);
    }

    public static class Sender implements Serializable {

        private static final long serialVersionUID = 4295317435660148765L;

        @NotNull
        private final Fields fields;

        @NotNull
        private final String streamId;

        public Sender(
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

    private static class Receiver implements IStreamReceiver, Serializable {

        private static final long serialVersionUID = 382619878420805508L;

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
