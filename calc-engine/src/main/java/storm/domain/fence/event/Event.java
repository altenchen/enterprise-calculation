package storm.domain.fence.event;

import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.domain.fence.Fence;
import storm.domain.fence.area.AreaSide;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.cron.Cron;

import java.util.function.Consumer;

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
     * 驶入电子围栏内部事件
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate 定位坐标
     * @param fence 电子围栏
     * @param event 事件
     * @param vehicleId 车辆标识
     * @param data 实时数据
     * @param cache 缓存数据
     * @param jsonNoticeCallback json 通知回调
     */
    default void gotoInsideEvent(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<String> jsonNoticeCallback) {}

    /**
     * 滞留电子围栏内部事件
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate 定位坐标
     * @param fence 电子围栏
     * @param event 事件
     * @param vehicleId 车辆标识
     * @param data 实时数据
     * @param cache 缓存数据
     * @param jsonNoticeCallback json 通知回调
     */
    default void keepInsideEvent(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<String> jsonNoticeCallback) {}

    /**
     * 驶入电子围栏外部事件
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate 定位坐标
     * @param fence 电子围栏
     * @param event 事件
     * @param vehicleId 车辆标识
     * @param data 实时数据
     * @param cache 缓存数据
     * @param jsonNoticeCallback json 通知回调
     */
    default void gotoOutsideEvent(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<String> jsonNoticeCallback) {}

    /**
     * 滞留电子围栏外部事件
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate 定位坐标
     * @param fence 电子围栏
     * @param event 事件
     * @param vehicleId 车辆标识
     * @param data 实时数据
     * @param cache 缓存数据
     * @param jsonNoticeCallback json 通知回调
     */
    default void keepOutsideEvent(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<String> jsonNoticeCallback) {}
}
