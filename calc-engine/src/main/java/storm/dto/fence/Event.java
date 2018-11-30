package storm.dto.fence;

import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * 事件
 * @author: xzp
 * @date: 2018-11-29
 * @description:
 */
public interface Event extends Cron {

    /**
     * 判断事件是否会触发
     * @param data 实时数据
     * @return true-事件被触发, false-事件未触发, null 数据无效.
     */
    @Nullable
    Boolean trigger(@NotNull final ImmutableMap<String, String> data);
}
