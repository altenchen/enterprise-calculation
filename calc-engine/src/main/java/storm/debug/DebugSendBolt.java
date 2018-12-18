package storm.debug;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.stream.IStreamReceiver;
import storm.stream.KafkaStream;
import storm.util.JedisPoolUtils;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-12-13
 * @description:
 */
public final class DebugSendBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DebugSendBolt.class);

    private static final JedisPoolUtils JEDIS_POOL_UTILS =
        JedisPoolUtils.getInstance();

    // region Component

    @NotNull
    private static final String COMPONENT_ID = DebugSendBolt.class.getSimpleName();

    @SuppressWarnings("unused")
    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private transient OutputCollector collector;

    private transient IStreamReceiver kafkaStreamReceiver;

    @Override
    public void prepare(
        @NotNull final Map stormConf,
        @NotNull final TopologyContext context,
        @NotNull final OutputCollector collector) {

        this.collector = collector;

        kafkaStreamReceiver = KafkaStream.prepareReceiver(this::executeFromKafkaStream);
    }

    @Override
    public void execute(
        @NotNull final Tuple input) {

        kafkaStreamReceiver.execute(input);
    }

    private void executeFromKafkaStream(
        @NotNull final Tuple input,
        @NotNull final String topic,
        @NotNull final String key,
        @NotNull final String message) {

        try {
            final String redisKey = "storm.debug.message." + topic + "." + key;
            JEDIS_POOL_UTILS.useResource(jedis -> {
                jedis.select(0);
                jedis.lpush(redisKey, message);
            });
            LOG.info("发送通知[ {} ] {}", topic, message);
            collector.ack(input);
        } catch (@NotNull final Exception e) {
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {
    }
}
