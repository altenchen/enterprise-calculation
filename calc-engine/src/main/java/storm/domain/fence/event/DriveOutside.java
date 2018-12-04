package storm.domain.fence.event;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.Fence;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.cron.Cron;

import java.util.function.Consumer;

/**
 * 驶离事件
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 */
public final class DriveOutside extends BaseEvent implements Event {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DriveOutside.class);

    public DriveOutside(
        @NotNull final String eventId,
        @Nullable final ImmutableCollection<Cron> cronSet) {

        super(eventId, cronSet);
    }

    @Override
    public void gotoOutsideEvent(
        final long platformReceiveTime, 
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data, 
        @NotNull final ImmutableMap<String, String> cache, 
        @NotNull final Consumer<String> jsonNoticeCallback) {
        // TODO 徐志鹏: 发送驶离通知
    }
}
