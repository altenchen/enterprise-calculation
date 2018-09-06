package storm.stream;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author: xzp
 * @date: 2018-09-05
 * @description: 流接收器过滤代理, 只有来自指定的组件和流的元组才会被处理.
 */
public final class StreamReceiverFilter implements IStreamReceiver, Serializable {

    private static final long serialVersionUID = -1896674425569805585L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(StreamReceiverFilter.class);

    private final String sourceComponentId;

    private final String sourceStreamId;

    private final IStreamReceiver streamReceiver;

    public StreamReceiverFilter(
        @NotNull final String sourceComponentId,
        @NotNull final String sourceStreamId,
        @NotNull final IStreamReceiver streamReceiver) {

        this.sourceComponentId = sourceComponentId;
        this.sourceStreamId = sourceStreamId;
        this.streamReceiver = streamReceiver;
    }

    @Override
    public void execute(
        final @NotNull Tuple input) {

        final String sourceComponentId = input.getSourceComponent();
        if(!StringUtils.equals(this.sourceComponentId,sourceComponentId)) {
            return;
        }

        final String sourceStreamId = input.getSourceStreamId();
        if(!StringUtils.equals(this.sourceStreamId,sourceStreamId)) {
            return;
        }

        streamReceiver.execute(input);
    }

    @Override
    @Contract("_, _ -> new")
    @NotNull
    public StreamReceiverFilter filter(
        final @NotNull String sourceComponentId,
        final @NotNull String sourceStreamId) {

        return new StreamReceiverFilter(
            sourceComponentId,
            sourceStreamId,
            streamReceiver);
    }
}
