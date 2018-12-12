package storm.domain.fence.notice;

import org.jetbrains.annotations.NotNull;
import storm.domain.fence.area.AreaSide;
import storm.domain.fence.event.EventStage;

/**
 * 延迟通知基类
 * @author: xzp
 * @date: 2018-12-12
 * @description:
 */
public abstract class BaseDelayNotice extends BaseNotice {

    /**
     * 触发事件开始需要的连续次数
     */
    public int beginThresholdTimes;

    /**
     * 触发事件开始需要的持续时长
     */
    public long beginTimeoutMillisecond;

    /**
     * 触发事件结束需要的连续次数
     */
    public int endThresholdTimes;

    /**
     * 触发事件结束需要的持续时长
     */
    public long endTimeoutMillisecond;

    public BaseDelayNotice(
        @NotNull final String messageId,
        @NotNull final String fenceId,
        @NotNull final String eventId,
        @NotNull final String vehicleId,
        @NotNull final String dataTime,
        final double longitude,
        final double latitude,
        @NotNull final AreaSide fromArea,
        @NotNull final AreaSide gotoArea,
        @NotNull final EventStage eventStage) {

        super(
            messageId,
            fenceId,
            eventId,
            vehicleId,
            dataTime,
            longitude,
            latitude,
            fromArea,
            gotoArea,
            eventStage
        );
    }

}
