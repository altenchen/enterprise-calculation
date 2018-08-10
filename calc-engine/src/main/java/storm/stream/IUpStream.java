package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description:
 */
public interface IUpStream extends IStreamDeclare {

    /**
     * 定义输出流
     * @param declarer
     */
    default void declareStream(@NotNull final OutputFieldsDeclarer declarer) {
        final String streamId = getStreamId();
        final Fields fields = getFields();
        declarer.declareStream(streamId, fields);
    }

    /**
     * 获取数据帧发射器
     * @param collector 输出采集器
     * @return 数据帧发射器
     */
    @NotNull
    default IStreamEmiter buildStreamEmiter(@NotNull final OutputCollector collector) {
        final String streamId = getStreamId();
        return new DefaultStreamEmiter(streamId, collector);
    }
}
