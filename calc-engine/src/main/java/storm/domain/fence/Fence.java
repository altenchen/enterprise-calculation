package storm.domain.fence;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.BooleanUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.area.Area;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.cron.Cron;
import storm.domain.fence.event.Event;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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

    public void process(
        @NotNull final Coordinate coordinate,
        final double distance,
        final long time,
        @NotNull final BiConsumer<Fence, Event> insideCallback,
        @NotNull final BiConsumer<Fence, Event> outsideCallback) {

        final Stream<Boolean> whichSideStream = areas
            .filter(area -> area.active(time))
            .map(area -> area.whichSide(coordinate, distance));

        if (whichSideStream.anyMatch(BooleanUtils::isTrue)) {
            rules
                .filter(event -> event.active(time))
                .forEachOrdered(event -> insideCallback.accept(this, event));
        } else if (whichSideStream.allMatch(Objects::nonNull)) {
            rules
                .filter(event -> event.active(time))
                .forEachOrdered(event -> outsideCallback.accept(this, event));
        }
    }

    @Override
    public boolean active(final long dateTime) {
        return cronSet.stream().anyMatch(cron -> cron.active(dateTime));
    }
}
