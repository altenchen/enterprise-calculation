package storm.domain.fence.event;

import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.domain.fence.cron.Cron;

import java.util.function.Consumer;

/**
 * 事件
 * @author: xzp
 * @date: 2018-11-29
 * @description:
 * 1. 每个事件可以包含多个激活时间段
 */
public interface Event extends Cron {

    /**
     * 获取事件标识
     * @return 事件标识
     */
    @NotNull
    String getEventId();

    /**
     * 进入电子围栏事件
     * @param data 实时数据
     * @param cache 缓存数据
     * @param noticeEmitter 通知发射器
     */
    default void gotoInsideEvent(
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<ImmutableMap<String, String>> noticeEmitter) {

    }

    /**
     * 判断事件是否会触发
     * @param whichSide 在电子围栏的内部或外部
     * @param data 实时数据
     * @param cache 缓存数据
     * @return true-事件被触发, false-事件未触发, null 数据无效.
     */
    @Nullable
    Boolean trigger(
        @Nullable final Boolean whichSide,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache);
}
