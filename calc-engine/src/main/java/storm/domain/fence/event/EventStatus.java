package storm.domain.fence.event;

import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import storm.domain.fence.Fence;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.notice.BaseNotice;

import java.util.function.Consumer;

/**
 * 事件状态
 * @author: xzp
 * @date: 2018-12-06
 * @description:
 */
public interface EventStatus {

    /**
     * 驶入电子围栏内部事件
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate 定位坐标
     * @param fence 电子围栏
     * @param event 事件
     * @param vehicleId 车辆标识
     * @param data 实时数据
     * @param cache 缓存数据
     * @param noticeCallback 通知回调
     */
    default void gotoInsideEvent(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<@NotNull ? super BaseNotice> noticeCallback) {}

    /**
     * 滞留电子围栏内部事件
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate 定位坐标
     * @param fence 电子围栏
     * @param event 事件
     * @param vehicleId 车辆标识
     * @param data 实时数据
     * @param cache 缓存数据
     * @param noticeCallback 通知回调
     */
    default void keepInsideEvent(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<@NotNull ? super BaseNotice> noticeCallback) {}

    /**
     * 驶入电子围栏外部事件
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate 定位坐标
     * @param fence 电子围栏
     * @param event 事件
     * @param vehicleId 车辆标识
     * @param data 实时数据
     * @param cache 缓存数据
     * @param noticeCallback 通知回调
     */
    default void gotoOutsideEvent(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<@NotNull ? super BaseNotice> noticeCallback) {}

    /**
     * 滞留电子围栏外部事件
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate 定位坐标
     * @param fence 电子围栏
     * @param event 事件
     * @param vehicleId 车辆标识
     * @param data 实时数据
     * @param cache 缓存数据
     * @param noticeCallback 通知回调
     */
    default void keepOutsideEvent(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<@NotNull ? super BaseNotice> noticeCallback) {}

    /**
     * 清理通知状态
     * @param noticeCallback 通知回调
     * @param reason 清理原因
     */
    default void cleanStatus(
        @NotNull final Consumer<@NotNull ? super BaseNotice> noticeCallback,
        @NotNull final String reason) {}
}
