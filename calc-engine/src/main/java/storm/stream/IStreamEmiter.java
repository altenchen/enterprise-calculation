package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description:
 */
public interface IStreamEmiter {

    /**
     * 获取流标识
     * @return 流标识
     */
    @NotNull
    String getStreamId();

    /**
     * 获取输出收集器
     * @return
     */
    @NotNull
    OutputCollector getOutputCollector();

    /**
     * 发射数据帧
     * @param values 数据帧
     */
    default void emit(@NotNull Values values) {
        final OutputCollector collector = getOutputCollector();
        final String streamId = getStreamId();
        collector.emit(streamId, values);
    }
}
