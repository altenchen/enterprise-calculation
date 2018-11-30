package storm.domain.fence;

import org.apache.commons.lang.BooleanUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.area.Area;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.cron.Cron;
import storm.domain.fence.event.Event;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * 电子围栏
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 * 1. 一个围栏包含多个有效区域
 * 2. 一个围栏包含多个有效规则
 * 3. 一个围栏包含多个激活时段
 */
public final class Fence implements Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(Fence.class);
    }

    @NotNull
    private final Stream<Area> areas;

    @NotNull
    private final Stream<Event> rules;

    /**
     * 激活计划
     */
    @NotNull
    private final Cron cron;

    public Fence(
        @NotNull final Stream<Area> areas,
        @NotNull final Stream<Event> rules,
        @Nullable final Cron cron) {

        this.areas = areas;
        this.rules = rules;
        this.cron = null != cron ? cron : Cron.DEFAULT;
    }

    public Fence(
        @NotNull final Stream<Area> areas,
        @NotNull final Stream<Event> rules) {

        this(areas, rules, Cron.DEFAULT);
    }

    public void process(
        @NotNull final Coordinate coordinate,
        final double distance,
        final long time,
        @NotNull final Consumer<Event> insideCallback,
        @NotNull final Consumer<Event> outsideCallback) {

        final Stream<Boolean> whichSideStream = areas
            .filter(area -> area.active(time))
            .map(area -> area.whichSide(coordinate, distance));

        if (whichSideStream.anyMatch(BooleanUtils::isTrue)) {
            rules
                .filter(event -> event.active(time))
                .forEachOrdered(insideCallback);
        } else if (whichSideStream.anyMatch(BooleanUtils::isFalse)) {
            rules
                .filter(event -> event.active(time))
                .forEachOrdered(outsideCallback);
        } else {
            // nothing......
        }
    }

    @Override
    public boolean active(final long time) {
        return cron.active(time);
    }
}
