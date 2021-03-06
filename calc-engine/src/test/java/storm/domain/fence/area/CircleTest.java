package storm.domain.fence.area;

import com.google.common.collect.ImmutableCollection;
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
@DisplayName("圆形区域测试")
final class CircleTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CircleTest.class);

    private CircleTest() {
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

    @DisplayName("测试圆形区域边界")
    @Test
    void testWhichSideBoundary() {
        final Coordinate center = new Coordinate(0, 0);
        final Coordinate location = new Coordinate(3, 4);
        final double distance = 1;

        {
            final double radius = 4 - 0.000001;
            final Circle circle = buildCircle(center, radius);
            Assertions.assertEquals(AreaSide.OUTSIDE, circle.computeAreaSide(location, 0, distance));
        }

        {
            final double radius = 4;
            final Circle circle = buildCircle(center, radius);
            Assertions.assertEquals(AreaSide.BOUNDARY, circle.computeAreaSide(location, 0, distance));
        }

        {
            final double radius = 6;
            final Circle circle = buildCircle(center, radius);
            Assertions.assertEquals(AreaSide.BOUNDARY, circle.computeAreaSide(location, distance, 0));
        }

        {
            final double radius = 6 + 0.000001;
            final Circle circle = buildCircle(center, radius);
            Assertions.assertEquals(AreaSide.INSIDE, circle.computeAreaSide(location, distance, 0));
        }
    }

    @DisplayName("测试圆形为实心")
    @Test
    void testWhichSideSolid() {
        final Coordinate center = new Coordinate(0, 0);
        final double radius = 5;
        final Circle circle = buildCircle(center, radius);

        final Coordinate location = center;

        {
            final double distance = radius + 0.000001;
            Assertions.assertEquals(AreaSide.BOUNDARY, circle.computeAreaSide(location, distance, 0));
        }

        {
            final double distance = radius;
            Assertions.assertEquals(AreaSide.BOUNDARY, circle.computeAreaSide(location, distance, 0));
        }

        {
            final double distance = radius - 0.000001;
            Assertions.assertEquals(AreaSide.INSIDE, circle.computeAreaSide(location, distance, 0));
        }
    }

    @Contract("_, _ -> new")
    @NotNull
    private Circle buildCircle(final Coordinate center, final double radius) {
        return new Circle(
            UUID.randomUUID().toString(),
            center,
            radius,
            null);
    }

    @Contract("_ -> new")
    @NotNull
    private Circle buildCircle(@Nullable final ImmutableCollection<Cron> cronSet) {
        return new Circle(
            UUID.randomUUID().toString(),
            new Coordinate(0, 0),
            0,
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
