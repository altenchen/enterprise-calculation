package storm.study;

import org.junit.jupiter.api.*;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-11-21
 * @description:
 */
@DisplayName("测试拓扑套件")
public final class JavaTopologySuiteTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(JavaTopologySuiteTest.class);

    private JavaTopologySuiteTest() {
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

    @Disabled("模拟电子围栏")
    @Test
    void testJts() {

        GeometryFactory factory = new GeometryFactory();

        // 创建 80x60 长方形
        final Geometry polygon = factory.createPolygon(new Coordinate[]{
            new Coordinate(0, 0),
            new Coordinate(80, 0),
            new Coordinate(80, 60),
            new Coordinate(0, 60),
            new Coordinate(0, 0)
        });

        {
            final Geometry point = factory.createPoint(new Coordinate(20, 30));

            // 点与边界的距离
            final double distance = polygon.getBoundary().distance(point);

            Assertions.assertEquals(20, distance);

            // 点是否在面内部
            final boolean contains = polygon.contains(point);

            Assertions.assertTrue(contains);

            Assertions.assertEquals(contains, polygon.buffer(20).contains(point));
        }

        {
            final Geometry point = factory.createPoint(new Coordinate(60, 30));

            // 点与边界的距离
            final double distance = polygon.getBoundary().distance(point);

            Assertions.assertEquals(20, distance);

            // 点是否在面内部
            final boolean contains = polygon.contains(point);

            Assertions.assertTrue(contains);

            Assertions.assertEquals(contains, polygon.buffer(20).contains(point));
        }

        {
            final Geometry point = factory.createPoint(new Coordinate(40, 20));

            // 点与边界的距离
            final double distance = polygon.getBoundary().distance(point);

            Assertions.assertEquals(20, distance);

            // 点是否在面内部
            final boolean contains = polygon.contains(point);

            Assertions.assertTrue(contains);

            Assertions.assertEquals(contains, polygon.buffer(20).contains(point));
        }

        {
            final Geometry point = factory.createPoint(new Coordinate(40, 40));

            // 点与边界的距离
            final double distance = polygon.getBoundary().distance(point);

            Assertions.assertEquals(20, distance);

            // 点是否在面内部
            final boolean contains = polygon.contains(point);

            Assertions.assertTrue(contains);

            Assertions.assertEquals(contains, polygon.buffer(20).contains(point));
        }

        {
            final Geometry point = factory.createPoint(new Coordinate(100, 30));

            // 点与边界的距离
            final double distance = polygon.getBoundary().distance(point);

            Assertions.assertEquals(20, distance);

            // 点是否在面内部
            final boolean contains = polygon.contains(point);

            Assertions.assertFalse(contains);

            Assertions.assertEquals(contains, polygon.buffer(20).contains(point));
        }

        {
            final Geometry point = factory.createPoint(new Coordinate(-20, 30));

            // 点与边界的距离
            final double distance = polygon.getBoundary().distance(point);

            Assertions.assertEquals(20, distance);

            // 点是否在面内部
            final boolean contains = polygon.contains(point);

            Assertions.assertFalse(contains);

            Assertions.assertEquals(contains, polygon.buffer(20).contains(point));
        }

        {
            final Geometry point = factory.createPoint(new Coordinate(40, 80));

            // 点与边界的距离
            final double distance = polygon.getBoundary().distance(point);

            Assertions.assertEquals(20, distance);

            // 点是否在面内部
            final boolean contains = polygon.contains(point);

            Assertions.assertFalse(contains);

            Assertions.assertEquals(contains, polygon.buffer(20).contains(point));
        }

        {
            final Geometry point = factory.createPoint(new Coordinate(40, -20));

            // 点与边界的距离
            final double distance = polygon.getBoundary().distance(point);

            Assertions.assertEquals(20, distance);

            // 点是否在面内部
            final boolean contains = polygon.contains(point);

            Assertions.assertFalse(contains);

            Assertions.assertEquals(contains, polygon.buffer(20).contains(point));
        }

        LOG.info("试验完成");
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
