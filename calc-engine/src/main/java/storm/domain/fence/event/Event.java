package storm.domain.fence.event;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.domain.fence.notice.BaseNotice;

import java.lang.reflect.Type;

/**
 * 事件
 * @author: xzp
 * @date: 2018-11-29
 * @description:
 */
public interface Event {

    /**
     * 获取事件标识
     * @return 事件标识
     */
    @NotNull
    String getEventId();

    /**
     * 获取通知类型
     * @return 通知类型
     */
    @NotNull
    Type getNoticeType();

    /**
     * 创建事件状态
     * @param notice 还原已开始未结束的通知
     * @return 事件状态
     */
    @NotNull
    EventStatus createEventStatus(@Nullable final BaseNotice notice);
}
