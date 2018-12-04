package storm.domain.fence.area;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.cron.Cron;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 多边形区域
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
public final class Polygon implements Area, Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(Polygon.class);

    @NotNull
    private final String areaId;

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
    private final ImmutableCollection<Cron> cronSet;

    public Polygon(
        @NotNull final String areaId,
        @NotNull final ImmutableList<Coordinate> shell,
        @Nullable final ImmutableCollection<Cron> cronSet) {

        this.areaId = areaId;

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

    @NotNull
    @Contract(pure = true)
    @Override
    public AreaSide whichSide(
        final @NotNull Coordinate coordinate,
        final double inSideDistance,
        final double outsideDistance) {

        final Geometry location = factory.createPoint(
            new org.locationtech.jts.geom.Coordinate(
                coordinate.longitude,
                coordinate.latitude)
        );

        final double distance = boundary.distance(location);

        if(polygon.contains(location)) {
            if(distance > Math.abs(inSideDistance)) {
                return AreaSide.INSIDE;
            } else {
                return AreaSide.BOUNDARY;
            }
        } else {
            if(distance > Math.abs(outsideDistance)) {
                return AreaSide.OUTSIDE;
            } else {
                return AreaSide.BOUNDARY;
            }
        }
    }

    @Override
    public boolean active(final long dateTime) {
        return cronSet.stream().anyMatch(cron -> cron.active(dateTime));
    }
}
