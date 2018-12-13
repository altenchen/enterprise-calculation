package storm.stream;

import com.google.common.collect.ImmutableMap;
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

/**
 * @author: xzp
 * @date: 2018-09-07
 * @description:
 */
public final class NoticeStream implements IStreamFields {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(NoticeStream.class);

    @NotNull
    private static final NoticeStream SINGLETON = new NoticeStream();

    @Contract(pure = true)
    public static NoticeStream getInstance() {
        return SINGLETON;
    }

    private NoticeStream() {
    }

    @NotNull
    private static final Fields FIELDS = new Fields(StreamFieldKey.VEHICLE_ID, StreamFieldKey.NOTICE);

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
            .append(NoticeStream.class.getSimpleName())
            .toString();
    }

    public void declareOutputFields(
        @NotNull final String streamId,
        @NotNull final OutputFieldsDeclarer declarer) {

        declarer.declareStream(streamId, FIELDS);
    }

    @Contract("_, _ -> new")
    @NotNull
    public NoticeStream.SpoutSender prepareSpoutSender(
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
            @NotNull final ImmutableMap<String, String> notice) {

            collector.emit(streamId, new Values(vid, notice));
        }

        public <T> void emit(
            @NotNull final MessageId<T> messageId,
            @NotNull final String vid,
            @NotNull final ImmutableMap<String, String> notice) {

            collector.emit(streamId, new Values(vid, notice), messageId);
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
            final ImmutableMap<String, String> notice =
                ((ImmutableMap<String, String>) input.getValueByField(StreamFieldKey.NOTICE));

            processor.execute(input, vid, notice);
        }
    }

    @FunctionalInterface
    public interface IProcessor {

        /**
         * 处理元组
         * @param input 输入元组
         * @param vid 车辆标识
         * @param notice 通知字典
         */
        void execute(
            @NotNull final Tuple input,
            @NotNull final String vid,
            @NotNull final ImmutableMap<String, String> notice);
    }
}
