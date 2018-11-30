package storm.domain.fence.area;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.cron.Cron;

/**
 * 圆形区域
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
public final class Circle implements Area, Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(Circle.class);

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
    private final Cron cron;

    public Circle(
        @NotNull final Coordinate center,
        final double radius,
        @Nullable final Cron cron) {

        this.center = center;
        this.radius = Math.abs(radius);
        this.cron = null != cron ? cron : Cron.DEFAULT;
    }

    public Circle(
        @NotNull final Coordinate center,
        final double radius) {

        this(center, radius, null);
    }

    @Nullable
    @Contract(pure = true)
    @Override
    public Boolean whichSide(@NotNull final Coordinate coordinate, final double distance) {

        final double width = coordinate.longitude - center.longitude;
        final double height = coordinate.latitude - center.latitude;

        final double width_square = width * width;
        final double height_square = height * height;
        final double radius_square = width_square + height_square;

        final double distance_abs = Math.abs(distance);

        final double outside = radius + distance_abs;
        final double outside_square = outside * outside;
        if(radius_square > outside_square) {
            return false;
        }

        final double inside = radius - distance_abs;
        if(inside > 0) {
            final double inside_square = inside * inside;
            if(radius_square < inside_square) {
                return true;
            }
        }

        return null;
    }

    @Override
    public boolean active(final long time) {
        return cron.active(time);
    }
}
                                                  