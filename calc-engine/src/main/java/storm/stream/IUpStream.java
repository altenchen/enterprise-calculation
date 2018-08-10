package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

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
    default void declareStream(@NotNull OutputFieldsDeclarer declarer) {
        declarer.declareStream(getStreamId(), getFields());
    }

    /**
     * 获取数据帧发射器
     * @param collector 输出采集器
     * @return 数据帧发射器
     */
    @NotNull
    default IStreamEmiter buildStreamEmiter(@NotNull OutputCollector collector) {
        final String streamId = getStreamId();
        return new DefaultStreamEmiter(streamId, collector);
    }
}
