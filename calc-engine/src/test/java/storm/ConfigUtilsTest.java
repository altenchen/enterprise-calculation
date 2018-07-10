package storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.ConfigUtils;
import org.apache.commons.collections.MapUtils;
import org.junit.jupiter.api.*;

/**
 * @author: xzp
 * @date: 2018-07-02
 * @description:
 */
@DisplayName("配置工具类测试")
final class ConfigUtilsTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(JedisTest.class);

    private ConfigUtilsTest() {}

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

    @Test
    @DisplayName("系统配置测试")
    void sysDefine() {
        final ConfigUtils configUtils = ConfigUtils.getInstance();
        Assertions.assertFalse(MapUtils.isEmpty(configUtils.sysDefine), "系统配置为空");
    }

    @Test
    @DisplayName("参数配置测试")
    void sysParams() {
        final ConfigUtils configUtils = ConfigUtils.getInstance();
        Assertions.assertFalse(MapUtils.isEmpty(configUtils.sysParams), "参数配置为空");
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
