package storm.stream;

import org.apache.storm.tuple.Fields;
import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-08-10
 * @description: 定义数据流字段
 */
public interface IStreamFields {

    /**
     * 获取数据帧包含的字段
     *
     * @return 数据帧包含的字段
     */
    @NotNull
    Fields getFields();

    /**
     * 获取流标识
     *
     * @param componentId 组件标识
     * @return 流标识
     */
    @NotNull
    default String getStreamId(@NotNull final String componentId) {

        final Fields fields = getFields();

        StringBuilder streamIdBuilder = new StringBuilder(128)
            .append(componentId)
            .append('-')
            .append(fields.get(0));

        for (int index = 1; index < fields.size(); index++) {
            streamIdBuilder
                .append('_')
                .append(
                    fields.get(index));
        }

        return streamIdBuilder.toString();
    }
}
