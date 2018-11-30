package storm.domain.fence.cron;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-11-30
 * @description:
 */
@DisplayName("一次性激活计划测试")
final class OnceTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(OnceTest.class);

    private OnceTest() {
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

    @DisplayName("测试一次性激活")
    @Test
    void testActive() {
        final long seed = System.currentTimeMillis();
        final long start = seed - 1000000;
        final long end = seed + 1000000;

        final Once once = new Once(start, end);

        Assertions.assertFalse(once.active(start - 1));
        Assertions.assertTrue(once.active(start));
        Assertions.assertTrue(once.active(end));
        Assertions.assertFalse(once.active(end + 1));

        LOG.trace("LOG.trace");
        LOG.debug("LOG.debug");
        LOG.info("LOG.info");
        LOG.warn("LOG.warn");
        LOG.error("LOG.error");

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
