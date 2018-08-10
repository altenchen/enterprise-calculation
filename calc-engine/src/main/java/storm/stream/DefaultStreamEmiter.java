package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description:
 */
public final class DefaultStreamEmiter implements IStreamEmiter {

    private final String streamId;
    private final OutputCollector collector;

    public DefaultStreamEmiter(final String streamId, final OutputCollector collector) {
        this.streamId = streamId;
        this.collector = collector;
    }

    @Contract(pure = true)
    @Override
    public @NotNull String getStreamId() {
        return streamId;
    }

    @Contract(pure = true)
    @Override
    public @NotNull OutputCollector getOutputCollector() {
        return collector;
    }
}
