package storm.kafka.bolt;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.FieldNameTopicSelector;
import org.apache.storm.kafka.bolt.selector.KafkaTopicSelector;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.bolt.deal.norm.FilterBolt;
import storm.stream.KafkaStream;
import storm.system.SysDefine;

import java.util.Properties;

/**
 * @author: xzp
 * @date: 2018-09-05
 * @description:
 */
public final class KafkaSendBolt extends KafkaBolt<String, String> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSendBolt.class);

    // region Component

    @NotNull
    private static final String COMPONENT_ID = KafkaSendBolt.class.getSimpleName();

    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    public KafkaSendBolt(
        @NotNull final String bootstrapServers) {

        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        withProducerProperties(producerProperties);

        final KafkaTopicSelector selector = new FieldNameTopicSelector(KafkaStream.TOPIC, null);
        withTopicSelector(selector);

        final TupleToKafkaMapper<String, String> mapper = new FieldNameBasedTupleToKafkaMapper<>(
            KafkaStream.BOLT_KEY,
            KafkaStream.BOLT_MESSAGE);
        withTupleToKafkaMapper(mapper);
    }
}
