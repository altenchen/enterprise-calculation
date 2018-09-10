package storm.kafka.spout;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.stream.RegisterStream;
import storm.stream.IStreamReceiver;
import storm.stream.StreamReceiverFilter;

/**
 * @author: xzp
 * @date: 2018-09-04
 * @description:
 */
public final class RegisterKafkaSpout extends KafkaSpout<String, String> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(RegisterKafkaSpout.class);
    // region Component

    @NotNull
    private static final String COMPONENT_ID = RegisterKafkaSpout.class.getSimpleName();

    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    // region RegisterStream

    @NotNull
    private static final RegisterStream REGISTER_STREAM = RegisterStream.getInstance();

    @NotNull
    private static final String REGISTER_STREAM_ID = REGISTER_STREAM.getStreamId(COMPONENT_ID);

    @NotNull
    @Contract(pure = true)
    public static String getRegisterStreamId() {
        return REGISTER_STREAM_ID;
    }

    @NotNull
    public static StreamReceiverFilter prepareRegisterStreamReceiver(
        @NotNull final RegisterStream.IProcessor processor) {

        return REGISTER_STREAM
            .prepareReceiver(
                processor)
            .filter(
                COMPONENT_ID,
                REGISTER_STREAM_ID);
    }

    @NotNull
    private static final RegisterStream.Sender REGISTER_STREAM_SENDER =
        REGISTER_STREAM.declareOutputFields(REGISTER_STREAM_ID);

    // endregion RegisterStream

    public RegisterKafkaSpout(
        @NotNull final String bootstrapServers,
        @NotNull final String consumerTopic,
        @NotNull final String consumerGroup) {

        super(KafkaSpoutConfig
            .builder(bootstrapServers, consumerTopic)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
            .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
            .setRecordTranslator(new RegisterRecordTranslator(REGISTER_STREAM_SENDER))
            .build());
    }
}
