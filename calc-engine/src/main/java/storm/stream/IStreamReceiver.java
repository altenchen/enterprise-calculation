package storm.stream;

import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-09-05
 * @description: 流接收器
 */
public interface IStreamReceiver {

    /**
     * 处理输入元组
     * @param input 输入元组
     */
    void execute(@NotNull final Tuple input);

    /**
     * 给当前接收器附加过滤器
     * @param sourceComponentId 输入元组来源组件
     * @param sourceStreamId 输入元组来源流
     * @return  附加了过滤器的接收器
     */
    default StreamReceiverFilter filter(
        @NotNull final String sourceComponentId,
        @NotNull final String sourceStreamId) {

        return new StreamReceiverFilter(
            sourceComponentId,
            sourceStreamId,
            this);
    }
}
