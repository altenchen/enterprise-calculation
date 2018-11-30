package storm.dto.fence;

import org.apache.commons.lang.BooleanUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * 电子围栏
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 * 1. 一个围栏包含多个有效区域
 * 2. 一个围栏包含多个有效时段
 * 3. 一个围栏包含多个有效规则
 */
public final class Fence {

    @SuppressWarnings("unused")
    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(Fence.class);
    }

    @NotNull
    private final Stream<Area> areas;

    @NotNull
    private final Stream<Event> rules;

    public Fence(
        @NotNull final Stream<Area> areas,
        @NotNull final Stream<Event> rules) {

        this.areas = areas;
        this.rules = rules;
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
}
