package storm.domain.fence;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.area.Area;
import storm.domain.fence.area.AreaSide;
import storm.domain.fence.area.BaseArea;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.cron.BaseCron;
import storm.domain.fence.cron.Cron;
import storm.domain.fence.event.BaseEvent;
import storm.domain.fence.event.Event;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * 电子围栏
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 * 1. 每个围栏关联多个有效区域
 * 2. 每个围栏关联多个有效规则
 * 3. 每个围栏关联多个激活时段
 * 4. 每个围栏关联多个车辆
 */
public final class Fence extends BaseCron implements Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(Fence.class);

    @NotNull
    private final String fenceId;

    @NotNull
    private final Stream<BaseArea> areas;

    @NotNull
    private final Stream<BaseEvent> rules;

    public Fence(
        @NotNull final String fenceId,
        @NotNull final Stream<BaseArea> areas,
        @NotNull final Stream<BaseEvent> rules,
        @Nullable final ImmutableCollection<Cron> cronSet) {

        super(cronSet);
        this.fenceId = fenceId;
        this.areas = areas;
        this.rules = rules;
    }

    /**
     * 获取电子围栏标识
     * @return 电子围栏标识
     */
    @NotNull
    @Contract(pure = true)
    public String getFenceId() {
        return fenceId;
    }

    /**
     * 处理电子围栏区域, 将 定位与围栏的关系 和 触发的事件 作为参数回调.
     * @param coordinate 数据定位
     * @param inSideDistance 坐标与区域边界的缓冲距离, 输入应该为零或正数
     * @param outsideDistance 坐标与区域边界的缓冲距离, 输入应该为零或正数
     * @param time 数据时间
     * @param whichSideCallback 围栏区域回调
     */
    public void process(
        @NotNull final Coordinate coordinate,
        final double inSideDistance,
        final double outsideDistance,
        final long time,
        @NotNull final BiConsumer<AreaSide, Event> whichSideCallback) {

        // 不在激活时间段, 跳过
        if(!active(time)) {
            return;
        }

        // 计算定位坐标与当前激活的区域的关系集合
        final Stream<AreaSide> whichSideStream = areas
            .filter(area -> area.active(time))
            .map(area -> area.computAreaSide(coordinate, inSideDistance, outsideDistance));

        // 当前激活的事件集合
        final Stream<BaseEvent> activeRuleStream = rules
            .filter(event -> event.active(time));

        if (whichSideStream.anyMatch(whichSide -> AreaSide.INSIDE == whichSide)) {
            activeRuleStream.forEachOrdered(
                event -> whichSideCallback.accept(AreaSide.INSIDE, event));
        } else if (whichSideStream.allMatch(whichSide -> AreaSide.OUTSIDE == whichSide)) {
            activeRuleStream.forEachOrdered(
                event -> whichSideCallback.accept(AreaSide.OUTSIDE, event));
        } else {
            activeRuleStream.forEachOrdered(
                event -> whichSideCallback.accept(AreaSide.BOUNDARY, event));
        }
    }
}
