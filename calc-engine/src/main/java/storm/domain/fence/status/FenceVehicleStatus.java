package storm.domain.fence.status;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.Fence;
import storm.domain.fence.area.AreaSide;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.event.Event;
import storm.domain.fence.event.EventStatus;
import storm.domain.fence.notice.BaseNotice;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * 围栏车辆状态
 *
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 */
public final class FenceVehicleStatus {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(FenceVehicleStatus.class);

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    @NotNull
    private final String fenceId;

    @NotNull
    private final String vehicleId;

    public FenceVehicleStatus(
        @NotNull final String fenceId,
        @NotNull final String vehicleId) {

        this.fenceId = fenceId;
        this.vehicleId = vehicleId;
    }

    // region EventStatus

    /**
     * <eventId, status>>
     */
    @NotNull
    private final Map<String, EventStatus> vehicleEventStatus = Maps.newHashMap();


    @NotNull
    private EventStatus ensureStatus(
        @NotNull final Event event
    ) {
        final String eventId = event.getEventId();
        final Type noticeType = event.getNoticeType();

        return vehicleEventStatus
            .computeIfAbsent(
                eventId,
                eid -> event.createEventStatus(
                    loadEventNotice(eid, noticeType)
                )
            );
    }

    @Contract(pure = true)
    @Nullable
    private BaseNotice loadEventNotice(
        @NotNull final String eventId,
        @NotNull final Type noticeType) {

        // TODO 许智杰: 从 redis 加载事件 noticeJson, 如果 redis 也没有, 则返回 null.
        final String json = "{}";

        return JSON_UTILS.fromJson(
            json,
            noticeType,
            e -> {
                LOG.warn("JSON反序列化[{}]到类型[{}]异常", json, noticeType, e);
                return null;
            });
    }

    @Contract(pure = true)
    @NotNull
    private void saveEventNotice(@NotNull BaseNotice notice) {

        final String eventId = notice.eventId;
        final String json = JSON_UTILS.toJson(notice);
        // TODO 许智杰: 将 noticeJson 持久化到 redis
    }

    public void cleanStatus(
        @NotNull BiPredicate<String, String> existEvent,
        @NotNull final Consumer<BaseNotice> noticeCallback) {

        vehicleEventStatus.entrySet().removeIf(eventStatus -> {
            final String eventId = eventStatus.getKey();

            if (existEvent.test(fenceId, eventId)) {
                return false;
            }

            // 清理无效的事件
            eventStatus.getValue().cleanStatus(noticeCallback);

            return true;
        });
    }

    // endregion EventStatus

    // region AreaSide

    @Nullable
    private AreaSide cacheSide = null;

    @NotNull
    private AreaSide loadAreaSide() {
        if (null == cacheSide) {
            // TODO 许智杰: cacheSide 如果为 null, 则从 redis 加载, 如果 redis 也没有, 则置为 AreaSide.UNKNOWN
            cacheSide = AreaSide.UNKNOWN;
        }
        return cacheSide;
    }

    private void saveAreaSide(@NotNull final AreaSide newSide) {

        if (AreaSide.INSIDE != newSide && AreaSide.OUTSIDE != newSide) {
            return;
        }

        final AreaSide oldSide = loadAreaSide();
        if (oldSide == newSide) {
            return;
        }

        cacheSide = newSide;

        // TODO 许智杰: 将 cacheSide 持久化到 redis
    }

    // endregion AreaSide

    /**
     * 处理电子围栏事件
     *
     * @param platformReceiveTime 数据的平台接收时间
     * @param coordinate          定位坐标
     * @param fence               电子围栏
     * @param whichSide           定位坐标与电子围栏的关系
     * @param activeEventMap      当前激活的事件
     * @param data                实时数据
     * @param cache               缓存数据
     * @param noticeCallback      json 通知回调
     */
    public void whichSideArea(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final AreaSide whichSide,
        @NotNull final ImmutableMap<String, Event> activeEventMap,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<BaseNotice> noticeCallback) {

        final Consumer<BaseNotice> noticeCallbackMiddleware =
            noticeCallback.andThen(this::saveEventNotice);

        switch (whichSide) {
            case INSIDE: {
                insideEvent(
                    platformReceiveTime,
                    coordinate,
                    fence,
                    activeEventMap,
                    data,
                    cache,
                    noticeCallbackMiddleware
                );
            }
            break;
            case OUTSIDE: {
                outsideArea(
                    platformReceiveTime,
                    coordinate,
                    fence,
                    activeEventMap,
                    data,
                    cache,
                    noticeCallbackMiddleware
                );
            }
            break;
            case BOUNDARY:
                // 不处理边界上的事件
                break;
            case UNKNOWN:
                LOG.warn(
                    "VID[{}]电子围栏计算得到未知区域, fence[{}]data[{}]cache[{}]",
                    vehicleId,
                    fence.getFenceId(),
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
        @NotNull final ImmutableMap<String, Event> activeEventMap,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<BaseNotice> noticeCallback) {

        switch (loadAreaSide()) {
            case OUTSIDE: {
                activeEventMap
                    .values()
                    .forEach(event ->
                        ensureStatus(event).gotoOutsideEvent(
                            platformReceiveTime,
                            coordinate,
                            fence,
                            event,
                            vehicleId,
                            data,
                            cache,
                            noticeCallback
                        )
                    );
            }
            break;
            case INSIDE: {
                activeEventMap
                    .values()
                    .forEach(event ->
                        ensureStatus(event).keepInsideEvent(
                            platformReceiveTime,
                            coordinate,
                            fence,
                            event,
                            vehicleId,
                            data,
                            cache,
                            noticeCallback
                        )
                    );
            }
            break;
            default:
                break;
        }
        saveAreaSide(AreaSide.INSIDE);
    }

    private void outsideArea(
        final long platformReceiveTime,
        @NotNull final Coordinate coordinate,
        @NotNull final Fence fence,
        @NotNull final ImmutableMap<String, Event> activeEventMap,
        @NotNull final ImmutableMap<String, String> data,
        @NotNull final ImmutableMap<String, String> cache,
        @NotNull final Consumer<BaseNotice> noticeCallback) {

        switch (loadAreaSide()) {
            case INSIDE: {
                activeEventMap
                    .values()
                    .forEach(event ->
                        ensureStatus(event).gotoInsideEvent(
                            platformReceiveTime,
                            coordinate,
                            fence,
                            event,
                            vehicleId,
                            data,
                            cache,
                            noticeCallback
                        )
                    );
            }
            break;
            case OUTSIDE: {
                activeEventMap
                    .values()
                    .forEach(event ->
                        ensureStatus(event).keepOutsideEvent(
                            platformReceiveTime,
                            coordinate,
                            fence,
                            event,
                            vehicleId,
                            data,
                            cache,
                            noticeCallback
                        )
                    );
            }
            break;
            default:
                break;
        }
        saveAreaSide(AreaSide.OUTSIDE);
    }
}
