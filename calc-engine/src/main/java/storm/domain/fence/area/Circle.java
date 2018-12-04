package storm.domain.fence.area;

import com.google.common.collect.ImmutableCollection;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.domain.fence.cron.Cron;

/**
 * 圆形区域
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
public final class Circle extends BaseArea implements Area, Cron {

    /**
     * 圆心
     */
    @NotNull
    private final Coordinate center;

    /**
     * 半径, 应当为零或正数
     */
    private final double radius;

    public Circle(
        @NotNull final String areaId,
        @NotNull final Coordinate center,
        final double radius,
        @Nullable final ImmutableCollection<Cron> cronSet) {

        super(areaId, cronSet);
        this.center = center;
        this.radius = Math.abs(radius);
    }

    @NotNull
    @Contract(pure = true)
    @Override
    public AreaSide computAreaSide(
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
            return AreaSide.OUTSIDE;
        }

        final double inside = radius - Math.abs(inSideDistance);
        if(inside > 0) {
            final double inside_square = inside * inside;
            if(radius_square < inside_square) {
                return AreaSide.INSIDE;
            }
        }

        return AreaSide.BOUNDARY;
    }
}
                                                  