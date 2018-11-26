package storm.bolt.deal.norm;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.stream.KafkaStream;
import storm.stream.StreamReceiverFilter;
import storm.system.SysDefine;

import java.util.Map;
import java.util.Objects;

/**
 * @author: xzp
 * @date: 2018-11-26
 * @description:
 */
public final class ElectronicFenceBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ElectronicFenceBolt.class);

    // region Component

    @NotNull
    private static final String COMPONENT_ID = ElectronicFenceBolt.class.getSimpleName();

    @SuppressWarnings("unused")
    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    // region KafkaStream

    @NotNull
    private static final KafkaStream KAFKA_STREAM = KafkaStream.getInstance();

    @NotNull
    private static final String KAFKA_STREAM_ID = KAFKA_STREAM.getStreamId(COMPONENT_ID);

    @NotNull
    @Contract(pure = true)
    public static String getKafkaStreamId() {
        return KAFKA_STREAM_ID;
    }

    // endregion KafkaStream

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private transient OutputCollector collector;

    private transient KafkaStream.Sender kafkaStreamFenceAlarmSender;

    private transient StreamReceiverFilter dataStreamReceiver;

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        prepareStreamSender(stormConf, collector);

        prepareStreamReceiver();
    }

    private void prepareStreamSender(
        @NotNull final Map stormConf,
        @NotNull final OutputCollector collector) {

        final KafkaStream.SenderBuilder kafkaStreamSenderBuilder = KAFKA_STREAM.prepareSender(KAFKA_STREAM_ID, collector);

        final String fenceAlarmTopic = Objects.toString(stormConf.get(SysDefine.KAFKA_PRODUCER_VEHICLE_FENCE_ALARM_TOPIC), null);
        if (StringUtils.isBlank(fenceAlarmTopic)) {
            LOG.error("电子围栏kafka输出主题为空, 请配置[{}]", SysDefine.KAFKA_PRODUCER_VEHICLE_FENCE_ALARM_TOPIC);
        }
        kafkaStreamFenceAlarmSender = kafkaStreamSenderBuilder.build(fenceAlarmTopic);
    }

    private void prepareStreamReceiver() {

        dataStreamReceiver = FilterBolt.prepareDataStreamReceiver(this::executeFromDataStream);
    }

    @Override
    public void execute(
        @NotNull final Tuple input) {

        if (dataStreamReceiver.execute(input)) {
            return;
        }

        collector.fail(input);
    }

    private void executeFromDataStream(
        @NotNull final Tuple input,
        @NotNull final String vid,
        @NotNull final ImmutableMap<String, String> data) {

        collector.ack(input);

        try {

        } catch (@NotNull final Exception e) {
            LOG.warn("电子围栏计算异常, VID[{}]", vid, e);
        }
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        KAFKA_STREAM.declareOutputFields(KAFKA_STREAM_ID, declarer);
    }
}
