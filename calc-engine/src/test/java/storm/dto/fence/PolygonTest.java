package storm.dto.fence;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
@DisplayName("多边形区域测试")
public final class PolygonTest {

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
        final Polygon polygon = new Polygon(
            ImmutableList
                .<Coordinate>builder()
                .add(new Coordinate(-10, 10))
                .add(new Coordinate(10, 10))
                .add(new Coordinate(10, -10))
                .add(new Coordinate(-10, -10))
                .add(new Coordinate(-10, 10))
                .build(), cron);
        final double distance = 5;

        // region top

        {
            final Coordinate location = new Coordinate(0, 15 + 0.000001);
            Assertions.assertEquals(Boolean.FALSE, polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(0, 15);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(0, 5);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(0, 5 - 0.000001);
            Assertions.assertEquals(Boolean.TRUE, polygon.whichSide(location, distance));
        }

        // endregion top

        // region top_right

        {
            final Coordinate location = new Coordinate(13 + 0.000001, 14 + 0.000001);
            Assertions.assertEquals(Boolean.FALSE, polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(13, 14);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(5, 5);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(5 - 0.000001, 5 - 0.000001);
            Assertions.assertEquals(Boolean.TRUE, polygon.whichSide(location, distance));
        }

        // endregion top_right

        // region right

        {
            final Coordinate location = new Coordinate(15 + 0.000001, 0);
            Assertions.assertEquals(Boolean.FALSE, polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(15, 0);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(5, 0);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(5 - 0.000001, 0);
            Assertions.assertEquals(Boolean.TRUE, polygon.whichSide(location, distance));
        }

        // endregion right

        // region right_bottom

        {
            final Coordinate location = new Coordinate(13 + 0.000001, -14 - 0.000001);
            Assertions.assertEquals(Boolean.FALSE, polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(13, -14);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(5, -5);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(5 - 0.000001, -5 + 0.000001);
            Assertions.assertEquals(Boolean.TRUE, polygon.whichSide(location, distance));
        }

        // endregion right_bottom

        // region bottom

        {
            final Coordinate location = new Coordinate(0, -15 - 0.000001);
            Assertions.assertEquals(Boolean.FALSE, polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(0, -15);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(0, -5);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(0, -5 + 0.000001);
            Assertions.assertEquals(Boolean.TRUE, polygon.whichSide(location, distance));
        }

        // endregion bottom

        // region bottom_left

        {
            final Coordinate location = new Coordinate(-13 - 0.000001, -14 - 0.000001);
            Assertions.assertEquals(Boolean.FALSE, polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(-13, -14);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(-5, -5);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(-5 + 0.000001, -5 + 0.000001);
            Assertions.assertEquals(Boolean.TRUE, polygon.whichSide(location, distance));
        }

        // endregion bottom_left

        // region left

        {
            final Coordinate location = new Coordinate(-15 - 0.000001, 0);
            Assertions.assertEquals(Boolean.FALSE, polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(-15, 0);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(-5, 0);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(-5 + 0.000001, 0);
            Assertions.assertEquals(Boolean.TRUE, polygon.whichSide(location, distance));
        }

        // endregion left

        // region left_top

        {
            final Coordinate location = new Coordinate(-13 - 0.000001, 14 + 0.000001);
            Assertions.assertEquals(Boolean.FALSE, polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(-13, 14);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(-5, 5);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        {
            final Coordinate location = new Coordinate(-5 + 0.000001, 5 - 0.000001);
            Assertions.assertEquals(Boolean.TRUE, polygon.whichSide(location, distance));
        }

        // endregion left_top
    }

    @DisplayName("测试多边形为实心")
    @Test
    void testWhichSideCorner() {
        final Polygon polygon = new Polygon(
            ImmutableList
                .<Coordinate>builder()
                .add(new Coordinate(-10, 10))
                .add(new Coordinate(10, 10))
                .add(new Coordinate(10, -10))
                .add(new Coordinate(-10, -10))
                .add(new Coordinate(-10, 10))
                .build(), cron);
        final double distance = 10;

        // region top

        {
            final Coordinate location = new Coordinate(0, - 0.000001);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        // endregion top

        // region top_right

        {
            final Coordinate location = new Coordinate(- 0.000001, - 0.000001);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        // endregion top_right

        // region right

        {
            final Coordinate location = new Coordinate(- 0.000001, 0);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        // endregion right

        // region right_bottom

        {
            final Coordinate location = new Coordinate(- 0.000001, 0.000001);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        // endregion right_bottom

        // region bottom

        {
            final Coordinate location = new Coordinate(0, 0.000001);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        // endregion bottom

        // region bottom_left

        {
            final Coordinate location = new Coordinate(0.000001, 0.000001);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        // endregion bottom_left

        // region left

        {
            final Coordinate location = new Coordinate(0.000001, 0);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        // endregion left

        // region left_top

        {
            final Coordinate location = new Coordinate(0.000001, - 0.000001);
            Assertions.assertNull(polygon.whichSide(location, distance));
        }

        // endregion left_top
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
