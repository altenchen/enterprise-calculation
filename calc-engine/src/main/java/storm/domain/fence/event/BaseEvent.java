package storm.domain.fence.event;

import com.google.common.collect.ImmutableCollection;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.domain.fence.cron.BaseCron;
import storm.domain.fence.cron.Cron;

/**
 * 事件基类, 实现了通用部分.
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 * 每个事件可以包含多个激活时间段
 */
public abstract class BaseEvent extends BaseCron implements EventCron, Event, Cron {

    @NotNull
    private final String eventId;

    BaseEvent(
        @NotNull final String eventId,
        @Nullable final ImmutableCollection<Cron> cronSet) {

        super(cronSet);
        this.eventId = eventId;
    }

    @Contract(pure = true)
    @NotNull
    @Override
    public final String getEventId() {
        return eventId;
    }
}
