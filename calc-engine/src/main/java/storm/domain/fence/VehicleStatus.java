package storm.domain.fence;

import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.area.AreaSide;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.event.Event;

import java.util.function.Consumer;

/**
 * 车辆状态
 *
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 */
public final class VehicleStatus {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(VehicleStatus.class);

    @NotNull
    private AreaSide cacheSide = AreaSide.UNKNOWN;

    public void clean(
        @NotNull final String fenceId,
        @NotNull final String eventId,
        @NotNull final String vehicleId) {
        // do nothing now...
    }

    /**
     * 处理电子围栏事件
     *
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate 定位坐标
     * @param fence 电子围栏
     * @param whichSide 定位坐标与电子围栏的关系
     * @param event 事件
     * @param vehicleId 车辆标识
     * @param data 实时数据
     * @param cache 缓存数据
     * @param jsonNoticeCallback  json 通知回调
     */
    public void whichSideArea(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final AreaSide whichSide,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<String> jsonNoticeCallback) {

        switch (whichSide) {
            case INSIDE: {
                insideEvent(
                    platformReceiveTime,
                    coordinate,
                    fence,
                    event,
                    vehicleId,
                    data,
                    cache,
                    jsonNoticeCallback
                );
            }
            break;
            case OUTSIDE: {
                outsideArea(
                    platformReceiveTime,
                    coordinate,
                    fence,
                    event,
                    vehicleId,
                    data,
                    cache,
                    jsonNoticeCallback
                );
            }
            break;
            case BOUNDARY:
                // 目前不处理边界上的事件
                break;
            case UNKNOWN:
                LOG.warn(
                    "VID[{}]电子围栏计算得到未知事件, fence[{}]event[{}]data[{}]cache[{}]",
                    vehicleId,
                    fence.getFenceId(),
                    event.getEventId(),
                    data,
                    cache);
                break;
            default:
                break;
        }
    }

    private void insideEvent(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<String> jsonNoticeCallback) {

        switch (cacheSide) {
            case OUTSIDE: {
                event.gotoOutsideEvent(
                    platformReceiveTime,
                    coordinate,
                    fence,
                    event,
                    vehicleId,
                    data,
                    cache,
                    jsonNoticeCallback
                );
            }
            break;
            case INSIDE: {
                event.keepInsideEvent(
                    platformReceiveTime,
                    coordinate,
                    fence,
                    event,
                    vehicleId,
                    data,
                    cache,
                    jsonNoticeCallback
                );
            }
            break;
            default:
                break;
        }
        cacheSide = AreaSide.INSIDE;
    }

    private void outsideArea(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final Event event,
        @NotNull final String vehicleId,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<String> jsonNoticeCallback) {

        switch (cacheSide) {
            case INSIDE: {
                event.gotoInsideEvent(
                    platformReceiveTime,
                    coordinate,
                    fence,
                    event,
                    vehicleId,
                    data,
                    cache,
                    jsonNoticeCallback
                );
            }
            break;
            case OUTSIDE: {
                event.keepOutsideEvent(
                    platformReceiveTime,
                    coordinate,
                    fence,
                    event,
                    vehicleId,
                    data,
                    cache,
                    jsonNoticeCallback
                );
            }
            break;
            default:
                break;
        }
        cacheSide = AreaSide.OUTSIDE;
    }
}
