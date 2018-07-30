package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import storm.constant.StreamFieldKey;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description:
 */
public class FromRegistToElasticsearchStream implements IStreamBase {

    @NotNull
    private static final Fields fields = new Fields(StreamFieldKey.VEHICLE_ID, StreamFieldKey.MSG);

    @NotNull
    private static final String componentId = "regpoutid";

    @NotNull
    private static final String streamId = "regStreamId";

    @NotNull
    private static final FromRegistToElasticsearchStream INSTANCE = new FromRegistToElasticsearchStream();

    @Contract(pure = true)
    public static FromRegistToElasticsearchStream getInstance() {
        return INSTANCE;
    }

    private FromRegistToElasticsearchStream() {

    }

    @Contract(pure = true)
    @NotNull
    @Override
    public Fields getFields() {
        return fields;
    }

    @Contract(pure = true)
    @NotNull
    @Override
    public String getComponentId() {
        return componentId;
    }

    @Contract(pure = true)
    @NotNull
    @Override
    public String getStreamId() {
        return streamId;
    }

    @Override
    public @NotNull FromGeneralToFilterStream.Emiter buildStreamEmiter(@NotNull final OutputCollector collector) {
        return new FromGeneralToFilterStream.Emiter(IStreamBase.super.buildStreamEmiter(collector));
    }

    public static final class Emiter implements IStreamEmiter {

        @NotNull
        private final IStreamEmiter emiter;

        public Emiter(@NotNull final IStreamEmiter emiter) {
            this.emiter = emiter;
        }

        @Override
        public @NotNull String getStreamId() {
            return emiter.getStreamId();
        }

        @Override
        public @NotNull OutputCollector getOutputCollector() {
            return emiter.getOutputCollector();
        }

        @Override
        public void emit(@NotNull final Values values) {
            emiter.emit(values);
        }

        public void emit(@NotNull final String vid, @NotNull final String msg) {
            emiter.emit(new Values(vid, msg));
        }
    }

    @NotNull
    public String getVid(@NotNull Tuple tuple) {
        return tuple.getStringByField(StreamFieldKey.VEHICLE_ID);
    }

    @NotNull
    public static String getMsg(@NotNull Tuple tuple) {
        return tuple.getStringByField(StreamFieldKey.MSG);
    }
}
