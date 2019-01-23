package storm.domain.fence.notice;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.domain.fence.area.AreaSide;
import storm.domain.fence.event.EventStage;

/**
 * 通知基类
 * @author: xzp
 * @date: 2018-12-05
 * @description:
 */
public abstract class BaseNotice {

    /**
     * 消息标识
     */
    @NotNull
    public final String messageId;

    /**
     * 获取围栏标识
     */
    @NotNull
    public final String fenceId;

    /**
     * 获取事件标识
     */
    @NotNull
    public final String eventId;

    /**
     * 获取车辆标识
     */
    @NotNull
    public final String vehicleId;

    /**
     * 数据时间
     */
    @NotNull
    public final String dataTime;

    /**
     * 经度
     */
    public final double longitude;

    /**
     * 纬度
     */
    public final double latitude;

    /**
     * 来自区域
     */
    @NotNull
    public final AreaSide fromArea;

    /**
     * 前往区域
     */
    @NotNull
    public final AreaSide gotoArea;

    /**
     * 事件状态
     */
    @NotNull
    public final EventStage eventStage;

    /**
     * 通知时间
     */
    @NotNull
    public String noticeTime;

    /**
     * 附加原因, 可用于描述一些非常规结束流程的原因
     */
    @Nullable
    public String reason;

    protected BaseNotice(
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

        this.messageId = messageId;
        this.fenceId = fenceId;
        this.eventId = eventId;
        this.vehicleId = vehicleId;
        this.dataTime = dataTime;
        this.longitude = longitude;
        this.latitude = latitude;
        this.fromArea = fromArea;
        this.gotoArea = gotoArea;
        this.eventStage = eventStage;
    }
}
