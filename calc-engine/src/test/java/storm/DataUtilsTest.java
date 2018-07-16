package storm;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.DataUtils;

/**
 * @author: xzp
 * @date: 2018-07-16
 * @description:
 */
@DisplayName("数据工具测试")
final class DataUtilsTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(DataUtilsTest.class);

    private DataUtilsTest() {
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

    @DisplayName("测试有效定位")
    @Test
    void isOrientationUseful() {
        final boolean orientationUseful = DataUtils.isOrientationUseful(0);
        Assertions.assertTrue(orientationUseful, "定位应该有效");
    }

    @DisplayName("测试无效定位")
    @Test
    void isOrientationUseless() {
        final boolean orientationUseless = DataUtils.isOrientationUseless(1);
        Assertions.assertTrue(orientationUseless, "定位应该无效");
    }

    @DisplayName("测试东经")
    @Test
    void isOrientationLongitudeEast() {
        final boolean orientationLongitudeEast = DataUtils.isOrientationLongitudeEast(0);
        Assertions.assertTrue(orientationLongitudeEast, "应该是东经");
    }

    @DisplayName("测试西经")
    @Test
    void isOrientationLongitudeWest() {
        final boolean orientationLongitudeWest = DataUtils.isOrientationLongitudeWest(4);
        Assertions.assertTrue(orientationLongitudeWest, "应该是西经");
    }

    @DisplayName("测试北纬")
    @Test
    void isOrientationLatitudeNorth() {
        final boolean orientationLatitudeNorth = DataUtils.isOrientationLatitudeNorth(0);
        Assertions.assertTrue(orientationLatitudeNorth, "应该是北纬");
    }

    @DisplayName("测试南纬")
    @Test
    void isOrientationLatitudeSouth() {
        final boolean orientationLatitudeSouth = DataUtils.isOrientationLatitudeSouth(2);
        Assertions.assertTrue(orientationLatitudeSouth, "应该是南纬");
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
