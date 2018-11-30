package storm.domain.fence.area;

import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.cron.Cron;

import java.util.stream.Collectors;

/**
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
public final class Polygon implements Area, Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(Polygon.class);

    @NotNull
    private final GeometryFactory factory;

    @NotNull
    private final Geometry polygon;

    @NotNull
    private final Geometry boundary;

    /**
     * 激活计划
     */
    @NotNull
    private final Cron cron;

    public Polygon(
        @NotNull final ImmutableList<Coordinate> shell,
        @Nullable final Cron cron) {

        factory = new GeometryFactory();
        polygon = factory
            .createPolygon(
                shell
                    .stream()
                    .map(coordinate ->
                        new org.locationtech.jts.geom.Coordinate(
                            coordinate.longitude,
                            coordinate.latitude)
                    )
                    .collect(Collectors.toList())
                    .toArray(new org.locationtech.jts.geom.Coordinate[shell.size()])
            );
        boundary = polygon.getBoundary();

        this.cron = null != cron ? cron : Cron.DEFAULT;
    }

    public Polygon(
        @NotNull final ImmutableList<Coordinate> shell) {

        this(shell, null);
    }

    @Nullable
    @Contract(pure = true)
    @Override
    public Boolean whichSide(final @NotNull Coordinate coordinate, final double distance) {

        final Geometry location = factory.createPoint(
            new org.locationtech.jts.geom.Coordinate(
                coordinate.longitude,
                coordinate.latitude)
        );

        if(boundary.distance(location) <= Math.abs(distance)) {
            return null;
        }

        return polygon.contains(location);
    }

    @Override
    public boolean active(final long dateTime) {
        return cron.active(dateTime);
    }
}
