package storm.kafka.spout;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.stream.GeneralStream;
import storm.stream.IStreamReceiver;

/**
 * @author: xzp
 * @date: 2018-09-04
 * @description:
 */
public final class GeneralKafkaSpout extends KafkaSpout<String, String> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(GeneralKafkaSpout.class);

    // region Component

    @NotNull
    private static final String COMPONENT_ID = GeneralKafkaSpout.class.getSimpleName();

    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    // region GeneralStream

    @NotNull
    private static final GeneralStream GENERAL_STREAM = GeneralStream.getInstance();

    @NotNull
    private static final String GENERAL_STREAM_ID = GENERAL_STREAM.getStreamId(COMPONENT_ID);

    @NotNull
    @Contract(pure = true)
    public static String getGeneralStreamId() {
        return GENERAL_STREAM_ID;
    }

    @NotNull
    public static IStreamReceiver prepareGeneralStreamReceiver(
        @NotNull final GeneralStream.IProcessor processor) {

        return GENERAL_STREAM
            .prepareReceiver(
                processor)
            .filter(
                COMPONENT_ID,
                GENERAL_STREAM_ID);
    }

    @NotNull
    private static final GeneralStream.Sender GENERAL_STREAM_SENDER =
        GENERAL_STREAM.declareOutputFields(GENERAL_STREAM_ID);

    // endregion GeneralStream

    public GeneralKafkaSpout(
        @NotNull final String bootstrapServers,
        @NotNull final String consumerTopic,
        @NotNull final String consumerGroup) {

        super(KafkaSpoutConfig
            .builder(bootstrapServers, consumerTopic)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
            .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
            .setRecordTranslator(new GeneralRecordTranslator(GENERAL_STREAM_SENDER))
            .build());
    }
}
