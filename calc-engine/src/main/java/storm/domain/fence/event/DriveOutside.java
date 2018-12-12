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
import storm.domain.fence.notice.DriveOutsideNotice;
import storm.tool.MultiDelaySwitch;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.lang.reflect.Type;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * 驶离事件
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 */
public final class DriveOutside extends BaseEvent implements Event {

    private static final Type NOTICE_TYPE = new TypeToken<DriveInsideNotice>() {
    }.getType();

    public DriveOutside(
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
        if (notice instanceof DriveOutsideNotice) {
            return new EventStatusImpl((DriveOutsideNotice) notice);
        }
        return new EventStatusImpl(null);
    }

    private static final class EventStatusImpl implements EventStatus {

        private final MultiDelaySwitch<EventStage> delaySwitch =
            new MultiDelaySwitch<EventStage>()
                .setThresholdTimes(
                    EventStage.BEGIN,
                    ConfigUtils.getSysDefine().getFenceEventDriveOutsideStartTriggerContinueCount())
                .setTimeoutMillisecond(
                    EventStage.BEGIN,
                    ConfigUtils.getSysDefine().getFenceEventDriveOutsideStartTriggerTimeoutMillisecond())
                .setThresholdTimes(
                    EventStage.END,
                    ConfigUtils.getSysDefine().getFenceEventDriveOutsideStopTriggerContinueCount())
                .setTimeoutMillisecond(
                    EventStage.END,
                    ConfigUtils.getSysDefine().getFenceEventDriveOutsideStopTriggerTimeoutMillisecond());

        @Nullable
        private DriveOutsideNotice notice;

        @Nullable
        private DriveOutsideNotice buffer;

        private EventStatusImpl(@Nullable final DriveOutsideNotice notice) {

            this.notice = notice;

            if (notice != null) {
                delaySwitch.setSwitchStatus(notice.eventStage);
            }
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

            delaySwitch.increase(
                EventStage.END,
                platformReceiveTime,
                status -> {

                    if(null == notice || EventStage.BEGIN != notice.eventStage) {
                        return;
                    }

                    buffer = new DriveOutsideNotice(
                        notice.messageId,
                        fence.getFenceId(),
                        event.getEventId(),
                        vehicleId,
                        DataUtils.buildFormatTime(platformReceiveTime),
                        coordinate.longitude,
                        coordinate.latitude,
                        AreaSide.OUTSIDE,
                        AreaSide.INSIDE,
                        EventStage.END);
                },
                (status, threshold, timeout) -> endOverflowCallback(threshold, timeout, noticeCallback)
            );
        }

        @Override
        public void keepInsideEvent(
            final long platformReceiveTime,
            @NotNull final Coordinate coordinate,
            @NotNull final Fence fence,
            @NotNull final Event event,
            @NotNull final String vehicleId,
            @NotNull final ImmutableMap<String, String> data,
            @NotNull final ImmutableMap<String, String> cache,
            @NotNull final Consumer<BaseNotice> noticeCallback) {

            delaySwitch.increase(
                EventStage.END,
                platformReceiveTime,
                status ->{},
                (status, threshold, timeout) -> endOverflowCallback(threshold, timeout, noticeCallback)
            );
        }

        private void endOverflowCallback(
            int threshold,
            long timeout,
            @NotNull final Consumer<BaseNotice> noticeCallback){

            if(null == notice || EventStage.BEGIN != notice.eventStage) {
                return;
            }

            if(null == buffer) {
                return;
            }

            buffer.beginThresholdTimes = notice.beginThresholdTimes;
            buffer.beginTimeoutMillisecond = notice.beginTimeoutMillisecond;
            buffer.endThresholdTimes = threshold;
            buffer.endTimeoutMillisecond = timeout;
            buffer.noticeTime = DataUtils.buildFormatTime(System.currentTimeMillis());

            noticeCallback.accept(buffer);

            notice = null;
            buffer = null;
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

            delaySwitch.increase(
                EventStage.BEGIN,
                platformReceiveTime,
                status ->{

                    if(null != notice && EventStage.BEGIN == notice.eventStage) {
                        return;
                    }

                    buffer = new DriveOutsideNotice(
                        UUID.randomUUID().toString(),
                        fence.getFenceId(),
                        event.getEventId(),
                        vehicleId,
                        DataUtils.buildFormatTime(platformReceiveTime),
                        coordinate.longitude,
                        coordinate.latitude,
                        AreaSide.INSIDE,
                        AreaSide.OUTSIDE,
                        EventStage.BEGIN);
                },
                (status, threshold, timeout) -> beginOverflowCallback(threshold, timeout, noticeCallback)
            );
        }

        @Override
        public void keepOutsideEvent(
            final long platformReceiveTime,
            @NotNull final Coordinate coordinate,
            @NotNull final Fence fence,
            @NotNull final Event event,
            @NotNull final String vehicleId,
            @NotNull final ImmutableMap<String, String> data,
            @NotNull final ImmutableMap<String, String> cache,
            @NotNull final Consumer<BaseNotice> noticeCallback) {

            delaySwitch.increase(
                EventStage.BEGIN,
                platformReceiveTime,
                status ->{},
                (status, threshold, timeout) -> beginOverflowCallback(threshold, timeout, noticeCallback)
            );
        }

        private void beginOverflowCallback(
            int threshold,
            long timeout,
            @NotNull final Consumer<BaseNotice> noticeCallback){

            if(null != notice && EventStage.BEGIN == notice.eventStage) {
                return;
            }

            if(null == buffer) {
                return;
            }

            buffer.beginThresholdTimes = threshold;
            buffer.beginTimeoutMillisecond = timeout;
            buffer.noticeTime = DataUtils.buildFormatTime(System.currentTimeMillis());

            noticeCallback.accept(buffer);

            notice = buffer;
            buffer = null;

        }
    }
}
