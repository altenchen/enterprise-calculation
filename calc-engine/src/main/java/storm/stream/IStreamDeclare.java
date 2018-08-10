package storm.stream;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description: 定义 storm stream
 */
public interface IStreamDeclare {

    /**
     * 获取组件标识
     * @return 组件标识
     */
    @NotNull
    String getComponentId();

    /**
     * 获取流字段
     * @return 流字段
     */
    @NotNull
    Fields getFields();

    /**
     * 获取流标识
     * @return 流标识
     */
    default String getStreamId() {
        return Utils.DEFAULT_STREAM_ID;
    }
}
