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
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.cron.Cron;
import storm.domain.fence.event.Event;
import storm.util.function.TeConsumer;

import java.util.Optional;
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
public final class Fence implements Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(Fence.class);

    @NotNull
    private final String fenceId;

    @NotNull
    private final Stream<Area> areas;

    @NotNull
    private final Stream<Event> rules;

    /**
     * 激活计划
     */
    @NotNull
    private final ImmutableCollection<Cron> cronSet;

    public Fence(
        @NotNull final String fenceId,
        @NotNull final Stream<Area> areas,
        @NotNull final Stream<Event> rules,
        @Nullable final ImmutableCollection<Cron> cronSet) {

        this.fenceId = fenceId;
        this.areas = areas;
        this.rules = rules;
        this.cronSet = Optional
            .ofNullable(cronSet)
            .orElseGet(
                () -> ImmutableSet.of(Cron.DEFAULT)
            );
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
     * 处理电子围栏逻辑
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
        @NotNull final TeConsumer<AreaSide, Fence, Event> whichSideCallback) {

        final Stream<AreaSide> whichSideStream = areas
            .filter(area -> area.active(time))
            .map(area -> area.whichSide(coordinate, inSideDistance, outsideDistance));

        final Stream<Event> activeRuleStream = rules
            .filter(event -> event.active(time));

        if (whichSideStream.anyMatch(whichSide -> AreaSide.INSIDE == whichSide)) {
            activeRuleStream
                .forEachOrdered(event -> whichSideCallback.accept(AreaSide.INSIDE,this, event));
        } else if (whichSideStream.allMatch(whichSide -> AreaSide.OUTSIDE == whichSide)) {
            activeRuleStream
                .forEachOrdered(event -> whichSideCallback.accept(AreaSide.OUTSIDE, this, event));
        } else {
            activeRuleStream
                .forEachOrdered(event -> whichSideCallback.accept(AreaSide.BOUNDARY, this, event));
        }
    }

    @Override
    public boolean active(final long dateTime) {
        return cronSet.stream().anyMatch(cron -> cron.active(dateTime));
    }

}
