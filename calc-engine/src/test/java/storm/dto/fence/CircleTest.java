package storm.dto.fence;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
@DisplayName("圆形区域测试")
public final class CircleTest {

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
            final Circle circle = new Circle(center, radius, cron);
            Assertions.assertEquals(Boolean.FALSE, circle.whichSide(location, distance));
        }

        {
            final double radius = 4;
            final Circle circle = new Circle(center, radius, cron);
            Assertions.assertNull(circle.whichSide(location, distance));
        }

        {
            final double radius = 6;
            final Circle circle = new Circle(center, radius, cron);
            Assertions.assertNull(circle.whichSide(location, distance));
        }

        {
            final double radius = 6 + 0.000001;
            final Circle circle = new Circle(center, radius, cron);
            Assertions.assertEquals(Boolean.TRUE, circle.whichSide(location, distance));
        }
    }

    @DisplayName("测试圆形为实心")
    @Test
    void testWhichSideSolid() {
        final Coordinate center = new Coordinate(0, 0);
        final double radius = 5;
        final Circle circle = new Circle(center, radius, cron);

        final Coordinate location = center;

        {
            final double distance = radius;
            Assertions.assertNull(circle.whichSide(location, distance));
        }

        {
            final double distance = radius - 0.000001;
            Assertions.assertEquals(Boolean.TRUE, circle.whichSide(location, distance));
        }


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
