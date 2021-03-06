package storm.domain.fence.notice;

import org.jetbrains.annotations.NotNull;
import storm.domain.fence.area.AreaSide;
import storm.domain.fence.event.EventStage;

/**
 * 默认通知
 * @author: xzp
 * @date: 2018-12-05
 * @description:
 */
public final class DefaultNotice extends BaseNotice {

    public DefaultNotice(
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
