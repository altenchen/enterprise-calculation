package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import storm.constant.StreamFieldKey;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description:
 */
public class FromGeneralToFilterStream implements IStreamBase {

    @NotNull
    private static final Fields fields = new Fields(StreamFieldKey.VEHICLE_ID, StreamFieldKey.MSG);

    @NotNull
    private static final String componentId = "realinfospoutid";

    @NotNull
    private static final FromGeneralToFilterStream INSTANCE = new FromGeneralToFilterStream();

    @Contract(pure = true)
    public static FromGeneralToFilterStream getInstance() {
        return INSTANCE;
    }

    private FromGeneralToFilterStream() {

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
    public Emiter buildStreamEmiter(@NotNull final OutputCollector collector) {
        return new Emiter(IStreamBase.super.buildStreamEmiter(collector));
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

        public void emit(@NotNull final String vid, @NotNull String msg) {
            emiter.emit(new Values(vid, msg));
        }
    }
}
