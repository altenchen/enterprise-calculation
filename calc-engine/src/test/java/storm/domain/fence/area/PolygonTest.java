package storm.domain.fence.area;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.cron.Cron;
import storm.domain.fence.cron.Daily;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
@DisplayName("多边形区域测试")
final class PolygonTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(PolygonTest.class);

    private PolygonTest() {
    }

    @SuppressWarnings("unused")
    @BeforeAll
    private static void beforeAll() {
        // 所有测试之前
    }

    @SuppressWarnings("unused")
    @BeforeEach
    private void beforeEach() {
        // 每个测试之前
    }

    @DisplayName("测试多边形区域边界")
    @Test
    void testWhichSideBoundary() {
        final Polygon polygon = buildPolygon(
            ImmutableList
                .<Coordinate>builder()
                .add(new Coordinate(-10, 10))
                .add(new Coordinate(10, 10))
                .add(new Coordinate(10, -10))
                .add(new Coordinate(-10, -10))
                .add(new Coordinate(-10, 10))
                .build());
        final double distance = 5;

        // region top

        {
            final Coordinate location = new Coordinate(0, 15 + 0.000001);
            Assertions.assertEquals(AreaSide.OUTSIDE, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(0, 15);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(0, 5);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        {
            final Coordinate location = new Coordinate(0, 5 - 0.000001);
            Assertions.assertEquals(AreaSide.INSIDE, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion top

        // region top_right

        {
            final Coordinate location = new Coordinate(13 + 0.000001, 14 + 0.000001);
            Assertions.assertEquals(AreaSide.OUTSIDE, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(13, 14);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(5, 5);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        {
            final Coordinate location = new Coordinate(5 - 0.000001, 5 - 0.000001);
            Assertions.assertEquals(AreaSide.INSIDE, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion top_right

        // region right

        {
            final Coordinate location = new Coordinate(15 + 0.000001, 0);
            Assertions.assertEquals(AreaSide.OUTSIDE, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(15, 0);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(5, 0);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        {
            final Coordinate location = new Coordinate(5 - 0.000001, 0);
            Assertions.assertEquals(AreaSide.INSIDE, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion right

        // region right_bottom

        {
            final Coordinate location = new Coordinate(13 + 0.000001, -14 - 0.000001);
            Assertions.assertEquals(AreaSide.OUTSIDE, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(13, -14);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(5, -5);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        {
            final Coordinate location = new Coordinate(5 - 0.000001, -5 + 0.000001);
            Assertions.assertEquals(AreaSide.INSIDE, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion right_bottom

        // region bottom

        {
            final Coordinate location = new Coordinate(0, -15 - 0.000001);
            Assertions.assertEquals(AreaSide.OUTSIDE, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(0, -15);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(0, -5);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        {
            final Coordinate location = new Coordinate(0, -5 + 0.000001);
            Assertions.assertEquals(AreaSide.INSIDE, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion bottom

        // region bottom_left

        {
            final Coordinate location = new Coordinate(-13 - 0.000001, -14 - 0.000001);
            Assertions.assertEquals(AreaSide.OUTSIDE, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(-13, -14);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(-5, -5);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        {
            final Coordinate location = new Coordinate(-5 + 0.000001, -5 + 0.000001);
            Assertions.assertEquals(AreaSide.INSIDE, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion bottom_left

        // region left

        {
            final Coordinate location = new Coordinate(-15 - 0.000001, 0);
            Assertions.assertEquals(AreaSide.OUTSIDE, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(-15, 0);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(-5, 0);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        {
            final Coordinate location = new Coordinate(-5 + 0.000001, 0);
            Assertions.assertEquals(AreaSide.INSIDE, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion left

        // region left_top

        {
            final Coordinate location = new Coordinate(-13 - 0.000001, 14 + 0.000001);
            Assertions.assertEquals(AreaSide.OUTSIDE, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(-13, 14);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, 0, distance));
        }

        {
            final Coordinate location = new Coordinate(-5, 5);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        {
            final Coordinate location = new Coordinate(-5 + 0.000001, 5 - 0.000001);
            Assertions.assertEquals(AreaSide.INSIDE, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion left_top
    }

    @DisplayName("测试多边形为实心")
    @Test
    void testWhichSideCorner() {
        final Polygon polygon = buildPolygon(
            ImmutableList
                .<Coordinate>builder()
                .add(new Coordinate(-10, 10))
                .add(new Coordinate(10, 10))
                .add(new Coordinate(10, -10))
                .add(new Coordinate(-10, -10))
                .add(new Coordinate(-10, 10))
                .build());
        final double distance = 10;

        // region top

        {
            final Coordinate location = new Coordinate(0, - 0.000001);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion top

        // region top_right

        {
            final Coordinate location = new Coordinate(- 0.000001, - 0.000001);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion top_right

        // region right

        {
            final Coordinate location = new Coordinate(- 0.000001, 0);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion right

        // region right_bottom

        {
            final Coordinate location = new Coordinate(- 0.000001, 0.000001);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion right_bottom

        // region bottom

        {
            final Coordinate location = new Coordinate(0, 0.000001);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion bottom

        // region bottom_left

        {
            final Coordinate location = new Coordinate(0.000001, 0.000001);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion bottom_left

        // region left

        {
            final Coordinate location = new Coordinate(0.000001, 0);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion left

        // region left_top

        {
            final Coordinate location = new Coordinate(0.000001, - 0.000001);
            Assertions.assertEquals(AreaSide.BOUNDARY, polygon.computeAreaSide(location, distance, 0));
        }

        // endregion left_top
    }

    @Contract("_ -> new")
    @NotNull
    private Polygon buildPolygon(@NotNull final ImmutableList<Coordinate> shell) {
        return new Polygon(
            UUID.randomUUID().toString(),
            shell,
            null);
    }

    @Contract("_ -> new")
    @NotNull
    private Polygon buildPolygon(@Nullable final ImmutableCollection<Cron> cronSet) {
        return new Polygon(
            UUID.randomUUID().toString(),
            ImmutableList.of(
                new Coordinate(-10, 10),
                new Coordinate(10, 10),
                new Coordinate(10, -10),
                new Coordinate(-10, -10),
                new Coordinate(-10, 10)
            ),
            cronSet);
    }

    @SuppressWarnings("unused")
    @AfterEach
    private void afterEach() {
        // 每个测试之后
    }

    @SuppressWarnings("unused")
    @AfterAll
    private static void afterAll() {
        // 所有测试之后
    }
}
