package storm.domain.fence;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.area.*;
import storm.domain.fence.cron.BaseCron;
import storm.domain.fence.event.*;

import java.lang.ref.Reference;
import java.util.Optional;
import java.util.UUID;

/**
 * @author: xzp
 * @date: 2018-12-07
 * @description:
 */
@DisplayName("电子围栏测试")
final class FenceTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(FenceTest.class);

    private FenceTest() {
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

    @DisplayName("测试处理方法——所有时间")
    @Test
    void testProcess() {

        // region 区域

        // 圆心(10, 10), 半径10
        // 与 x轴 相交于 (0,5)
        // 与 y轴 相交于 (5, 0)
        final Circle circle = new Circle(
            UUID.randomUUID().toString(),
            new Coordinate(10, 10),
            10,
            null);

        // 正方形, 边长为20
        // 10 <= x <= 30
        // 10 <= y <= 30
        // 与圆相交于 (10, 20) 和 (20, 10)
        final Polygon polygon = new Polygon(
            UUID.randomUUID().toString(),
            ImmutableList.of(
                new Coordinate(10, 10),
                new Coordinate(10, 30),
                new Coordinate(30, 30),
                new Coordinate(30, 10),
                new Coordinate(10, 10)
            ),
            null
        );

        // endregion 区域

        final Fence fence = new Fence(
            UUID.randomUUID().toString(),
            ImmutableMap.of(
                circle.getAreaId(), circle,
                polygon.getAreaId(), polygon
            ),
            ImmutableMap.of(),
            null);
        final double inSideDistance = 0;
        final double outsideDistance = 0;
        final long time = System.currentTimeMillis();

        // 内部 > 边界 > 外部

        // region 圆形内部,多边形内部 -> 在内部

        {
            final boolean[] isCallback = new boolean[1];

            fence.process(
                new Coordinate(10 + 0.000001, 10 + 0.000001),
                inSideDistance,
                outsideDistance,
                time,
                (areaSide, activeEventMap) -> {
                    isCallback[0] = true;
                    Assertions.assertEquals(AreaSide.INSIDE, areaSide);
                });

            Assertions.assertTrue(isCallback[0]);
        }

        // endregion 圆形内部,多边形内部 -> 在内部

        // region 圆形内部,多边形边界 -> 在内部

        {
            final boolean[] isCallback = new boolean[1];

            fence.process(
                new Coordinate(10, 10),
                inSideDistance,
                outsideDistance,
                time,
                (areaSide, activeEventMap) -> {
                    isCallback[0] = true;
                    Assertions.assertEquals(AreaSide.INSIDE, areaSide);
                });

            Assertions.assertTrue(isCallback[0]);
        }

        // endregion 圆形内部,多边形边界 -> 在内部

        // region 圆形内部,多边形外部 -> 在内部

        {
            final boolean[] isCallback = new boolean[1];

            fence.process(
                new Coordinate(10 - 0.000001, 10 - 0.000001),
                inSideDistance,
                outsideDistance,
                time,
                (areaSide, activeEventMap) -> {
                    isCallback[0] = true;
                    Assertions.assertEquals(AreaSide.INSIDE, areaSide);
                });

            Assertions.assertTrue(isCallback[0]);
        }

        // endregion 圆形内部,多边形外部 -> 在内部


        // region 圆形边界,多边形内部 -> 在内部

        {
            final boolean[] isCallback = new boolean[1];

            fence.process(
                new Coordinate(10 + 6, 10 + 8),
                inSideDistance,
                outsideDistance,
                time,
                (areaSide, activeEventMap) -> {
                    isCallback[0] = true;
                    Assertions.assertEquals(AreaSide.INSIDE, areaSide);
                });

            Assertions.assertTrue(isCallback[0]);
        }

        // endregion 圆形边界,多边形内部 -> 在内部

        // region 圆形边界,多边形边界 -> 在边界

        {
            final boolean[] isCallback = new boolean[1];

            fence.process(
                new Coordinate(10, 20),
                inSideDistance,
                outsideDistance,
                time,
                (areaSide, activeEventMap) -> {
                    isCallback[0] = true;
                    Assertions.assertEquals(AreaSide.BOUNDARY, areaSide);
                });

            Assertions.assertTrue(isCallback[0]);
        }

        // endregion 圆形边界,多边形边界 -> 在边界

        // region 圆形边界,多边形外部 -> 在边界

        {
            final boolean[] isCallback = new boolean[1];

            fence.process(
                new Coordinate(0, 10),
                inSideDistance,
                outsideDistance,
                time,
                (areaSide, activeEventMap) -> {
                    isCallback[0] = true;
                    Assertions.assertEquals(AreaSide.BOUNDARY, areaSide);
                });

            Assertions.assertTrue(isCallback[0]);
        }

        // endregion 圆形边界,多边形外部 -> 在边界


        // region 圆形外部,多边形内部 -> 在内部

        {
            final boolean[] isCallback = new boolean[1];

            fence.process(
                new Coordinate(30 - 0.000001, 30 - 0.000001),
                inSideDistance,
                outsideDistance,
                time,
                (areaSide, activeEventMap) -> {
                    isCallback[0] = true;
                    Assertions.assertEquals(AreaSide.INSIDE, areaSide);
                });

            Assertions.assertTrue(isCallback[0]);
        }

        // endregion 圆形外部,多边形内部 -> 在内部

        // region 圆形外部,多边形边界 -> 在边界

        {
            final boolean[] isCallback = new boolean[1];

            fence.process(
                new Coordinate(30, 30),
                inSideDistance,
                outsideDistance,
                time,
                (areaSide, activeEventMap) -> {
                    isCallback[0] = true;
                    Assertions.assertEquals(AreaSide.BOUNDARY, areaSide);
                });

            Assertions.assertTrue(isCallback[0]);
        }

        // endregion 圆形外部,多边形边界 -> 在边界

        // region 圆形外部,多边形外部 -> 在外部

        {
            final boolean[] isCallback = new boolean[1];

            fence.process(
                new Coordinate(30 + 0.000001, 30 + 0.000001),
                inSideDistance,
                outsideDistance,
                time,
                (areaSide, activeEventMap) -> {
                    isCallback[0] = true;
                    Assertions.assertEquals(AreaSide.OUTSIDE, areaSide);
                });

            Assertions.assertTrue(isCallback[0]);
        }

        // endregion 圆形外部,多边形外部 -> 在外部

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
