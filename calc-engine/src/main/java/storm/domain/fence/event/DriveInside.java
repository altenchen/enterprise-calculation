package storm.domain.fence.event;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.domain.fence.Fence;
import storm.domain.fence.area.AreaSide;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.cron.Cron;
import storm.domain.fence.notice.BaseNotice;
import storm.domain.fence.notice.DriveInsideNotice;
import storm.util.DataUtils;

import java.lang.reflect.Type;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * 驶入事件
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 */
public final class DriveInside extends BaseEvent implements Event  {

    private static final Type NOTICE_TYPE = new TypeToken<DriveInsideNotice>() {
    }.getType();

    public DriveInside(
        @NotNull final String eventId,
        @Nullable final ImmutableCollection<Cron> cronSet) {
        super(eventId, cronSet);
    }

    @Contract(pure = true)
    @NotNull
    @Override
    public Type getNoticeType() {
        return NOTICE_TYPE;
    }

    @Contract("null -> new")
    @NotNull
    @Override
    public EventStatus createEventStatus(@Nullable final BaseNotice notice) {
        if (notice instanceof DriveInsideNotice) {
            return new EventStatusImpl((DriveInsideNotice) notice);
        }
        return new EventStatusImpl(null);
    }

    private static final class EventStatusImpl implements EventStatus {

        @Nullable
        private DriveInsideNotice notice;

        private EventStatusImpl(@Nullable final DriveInsideNotice notice) {

            this.notice = notice;
        }

        @Override
        public void gotoInsideEvent(
            final long platformReceiveTime,
            @NotNull final Coordinate coordinate,
            @NotNull final Fence fence,
            @NotNull final Event event,
            @NotNull final String vehicleId,
            @NotNull final ImmutableMap<String, String> data,
            @NotNull final ImmutableMap<String, String> cache,
            @NotNull final Consumer<BaseNotice> noticeCallback) {

            if(null != notice) {
                return;
            }

            final DriveInsideNotice beginNotice = new DriveInsideNotice(
                UUID.randomUUID().toString(),
                fence.getFenceId(),
                event.getEventId(),
                vehicleId,
                DataUtils.buildFormatTime(platformReceiveTime),
                coordinate.longitude,
                coordinate.latitude,
                AreaSide.OUTSIDE,
                AreaSide.INSIDE,
                EventStage.BEGIN,
                DataUtils.buildFormatTime(System.currentTimeMillis())
            );

            noticeCallback.accept(beginNotice);

            notice = beginNotice;
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
            @NotNull final Consumer<BaseNotice> noticeCallback) {

            if(null == notice) {
                return;
            }

            final DriveInsideNotice endNotice = new DriveInsideNotice(
                notice.messageId,
                fence.getFenceId(),
                event.getEventId(),
                vehicleId,
                DataUtils.buildFormatTime(platformReceiveTime),
                coordinate.longitude,
                coordinate.latitude,
                AreaSide.INSIDE,
                AreaSide.OUTSIDE,
                EventStage.END,
                DataUtils.buildFormatTime(System.currentTimeMillis())
            );

            noticeCallback.accept(endNotice);

            notice = null;
        }
    }
}
