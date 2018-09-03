package storm.stream;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.KafkaTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author: xzp
 * @date: 2018-08-12
 * @description:
 */
public final class KafkaStream implements IStreamFields, Serializable {

    private static final long serialVersionUID = 7144846979444508983L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStream.class);

    @NotNull
    private static final KafkaStream SINGLETON = new KafkaStream();

    @Contract(pure = true)
    public static KafkaStream getInstance() {
        return SINGLETON;
    }

    private KafkaStream() {
    }

    public static final String TOPIC = KafkaBolt.TOPIC;

    public static final String BOLT_KEY = FieldNameBasedTupleToKafkaMapper.BOLT_KEY;

    public static final String BOLT_MESSAGE = FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE;

    @NotNull
    private static final Fields FIELDS = new Fields(TOPIC, BOLT_KEY, BOLT_MESSAGE);

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
            .append(KafkaStream.class.getSimpleName())
            .toString();
    }

    public void declareOutputFields(
        @NotNull final String streamId,
        @NotNull final OutputFieldsDeclarer declarer) {

        declarer.declareStream(streamId, FIELDS);
    }

    @Contract("_, _ -> new")
    @NotNull
    public SenderBuilder prepareSender(
        @NotNull final String streamId,
        @NotNull final OutputCollector collector) {

        return new SenderBuilder(streamId, collector);
    }

    public static class SenderBuilder implements Serializable {

        private static final long serialVersionUID = -8014503001829782489L;

        @NotNull
        private final String streamId;

        @NotNull
        private final OutputCollector collector;

        public SenderBuilder(
            @NotNull final String streamId,
            @NotNull final OutputCollector collector) {

            this.streamId = streamId;
            this.collector = collector;
        }

        public void emit(
            @NotNull final String topic,
            @NotNull final String key,
            @NotNull final String message) {

            collector.emit(
                streamId,
                new KafkaTuple(topic, key, message)
                    .routedTo(streamId));
        }

        public void emit(
            @NotNull final Tuple anchors,
            @NotNull final String topic,
            @NotNull final String key,
            @NotNull final String message) {

            collector.emit(
                streamId,
                anchors,
                new KafkaTuple(topic, key, message)
                    .routedTo(streamId));
        }

        @NotNull
        public Sender build(@NotNull final String topic) {

            return new Sender(streamId, collector, topic);
        }
    }

    public static class Sender implements Serializable {

        private static final long serialVersionUID = -3287896215787237543L;

        @NotNull
        private final String streamId;

        @NotNull
        private final OutputCollector collector;

        @NotNull
        private final String topic;

        public Sender(
            @NotNull final String streamId,
            @NotNull final OutputCollector collector,
            @NotNull final String topic) {

            this.streamId = streamId;
            this.collector = collector;
            this.topic = topic;
        }

        public void emit(
            @NotNull final String key,
            @NotNull final String message) {

            collector.emit(
                streamId,
                new KafkaTuple(topic, key, message)
                    .routedTo(streamId));
        }

        public void emit(
            @NotNull final Tuple anchors,
            @NotNull final String key,
            @NotNull final String message) {

            collector.emit(
                streamId,
                anchors,
                new KafkaTuple(topic, key, message)
                    .routedTo(streamId));
        }
    }


    public static void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer,
        @NotNull final String streamId) {

        declarer.declareStream(
            streamId,
            new Fields(
                KafkaStream.TOPIC,
                KafkaStream.BOLT_KEY,
                KafkaStream.BOLT_MESSAGE));
    }

    public static void emit(
        @NotNull final OutputCollector collector,
        @NotNull final String streamId,
        @NotNull final String topic,
        @NotNull final String key,
        @NotNull final String message) {

        collector.emit(streamId, new KafkaTuple(topic, key, message).routedTo(streamId));
    }
}
