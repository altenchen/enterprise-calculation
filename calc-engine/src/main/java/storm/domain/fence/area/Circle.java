package storm.domain.fence.area;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.cron.Cron;

import java.util.Optional;

/**
 * 圆形区域
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
public final class Circle implements Area, Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(Circle.class);

    @NotNull
    private final String areaId;

    /**
     * 圆心
     */
    @NotNull
    private final Coordinate center;

    /**
     * 半径, 应当为零或正数
     */
    private final double radius;

    /**
     * 激活计划
     */
    @NotNull
    private final ImmutableCollection<Cron> cronSet;

    public Circle(
        @NotNull final String areaId,
        @NotNull final Coordinate center,
        final double radius,
        @Nullable final ImmutableCollection<Cron> cronSet) {

        this.areaId = areaId;
        this.center = center;
        this.radius = Math.abs(radius);
        this.cronSet = Optional
            .ofNullable(cronSet)
            .orElseGet(
                () -> ImmutableSet.of(Cron.DEFAULT)
            );
    }

    @NotNull
    @Contract(pure = true)
    @Override
    public String getAreaId() {
        return areaId;
    }

    @Nullable
    @Contract(pure = true)
    @Override
    public Boolean whichSide(
        @NotNull final Coordinate coordinate,
        final double inSideDistance,
        final double outsideDistance) {

        final double width = coordinate.longitude - center.longitude;
        final double height = coordinate.latitude - center.latitude;

        final double width_square = width * width;
        final double height_square = height * height;
        final double radius_square = width_square + height_square;

        final double outside = radius + Math.abs(outsideDistance);
        final double outside_square = outside * outside;
        if(radius_square > outside_square) {
            return Boolean.FALSE;
        }

        final double inside = radius - Math.abs(inSideDistance);
        if(inside > 0) {
            final double inside_square = inside * inside;
            if(radius_square < inside_square) {
                return Boolean.TRUE;
            }
        }

        return null;
    }

    @Override
    public boolean active(final long dateTime) {
        return cronSet.stream().anyMatch(cron -> cron.active(dateTime));
    }
}
                                                  