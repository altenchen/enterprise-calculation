package storm.stream;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description: 定义 storm stream
 */
public interface IStreamDeclare {

    /**
     * 获取流标识
     * @return 流标识
     */
    @NotNull
    String getStreamId();

    /**
     * 获取流字段
     * @return 流字段
     */
    @NotNull
    Fields getFields();
}
