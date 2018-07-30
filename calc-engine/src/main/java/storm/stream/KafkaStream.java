package storm.stream;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.KafkaTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-08-12
 * @description:
 */
public final class KafkaStream {

    public static final String TOPIC = KafkaBolt.TOPIC;

    public static final String BOLT_KEY = FieldNameBasedTupleToKafkaMapper.BOLT_KEY;

    public static final String BOLT_MESSAGE = FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE;

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
