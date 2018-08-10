package storm.stream;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description:
 */
public interface IDownStream extends IStreamDeclare {

    /**
     * 判断当前流是不是元组的来源
     * @param tuple 元组
     * @return 当前流是不是元组的来源
     */
    default boolean isSourceStream(@NotNull Tuple tuple) {
        final String sourceStreamId = tuple.getSourceStreamId();
        final String streamId = getStreamId();
        return StringUtils.equals(streamId, sourceStreamId);
    }
}
