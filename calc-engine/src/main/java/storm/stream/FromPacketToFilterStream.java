package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import storm.constant.StreamFieldKey;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description:
 */
public class FromPacketToFilterStream implements IStreamBase {

    @NotNull
    private static final Fields fields = new Fields(StreamFieldKey.VEHICLE_ID, StreamFieldKey.MSG, StreamFieldKey.FLAG);

    @NotNull
    private static final String componentId = "errordataspoutid";

    @NotNull
    private static final FromPacketToFilterStream INSTANCE = new FromPacketToFilterStream();

    @Contract(pure = true)
    public static FromPacketToFilterStream getInstance() {
        return INSTANCE;
    }

    private FromPacketToFilterStream() {

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

        public void emit(@NotNull final String vid, @NotNull final Map<String, String> msg) {
            emit(vid, msg, 1);
        }

        public void emit(@NotNull final String vid, @NotNull final Map<String, String> msg, final int flag) {
            emiter.emit(new Values(vid, msg, flag));
        }
    }
}
