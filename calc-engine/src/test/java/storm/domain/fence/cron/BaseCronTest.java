package storm.domain.fence.cron;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.area.Circle;

import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 */
@DisplayName("计划基类测试")
final class BaseCronTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(BaseCronTest.class);

    private BaseCronTest() {
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

    @DisplayName("测试时间范围")
    @Test
    void testActive() {
        final Daily forenoon = new Daily(
            TimeUnit.HOURS.toMillis(9),
            TimeUnit.HOURS.toMillis(12));
        final Daily afternoon = new Daily(
            TimeUnit.HOURS.toMillis(13),
            TimeUnit.HOURS.toMillis(18));
        final Cron cron = new BaseCron(ImmutableSet.of(forenoon, afternoon)){};

        Assertions.assertFalse(cron.active(TimeUnit.HOURS.toMillis(9) - 1));
        Assertions.assertTrue(cron.active(TimeUnit.HOURS.toMillis(9)));
        Assertions.assertTrue(cron.active(TimeUnit.HOURS.toMillis(9) + 1));
        Assertions.assertTrue(cron.active(TimeUnit.HOURS.toMillis(12) - 1));
        Assertions.assertFalse(cron.active(TimeUnit.HOURS.toMillis(12)));
        Assertions.assertFalse(cron.active(TimeUnit.HOURS.toMillis(12) + 1));
        Assertions.assertFalse(cron.active(TimeUnit.HOURS.toMillis(13) - 1));
        Assertions.assertTrue(cron.active(TimeUnit.HOURS.toMillis(13)));
        Assertions.assertTrue(cron.active(TimeUnit.HOURS.toMillis(13) + 1));
        Assertions.assertTrue(cron.active(TimeUnit.HOURS.toMillis(18) - 1));
        Assertions.assertFalse(cron.active(TimeUnit.HOURS.toMillis(18)));
        Assertions.assertFalse(cron.active(TimeUnit.HOURS.toMillis(18) + 1));
    }

    @DisplayName("测试空时间")
    @Test
    void testEmpty() {
        Assertions.assertTrue(new BaseCron(null){}.active(TimeUnit.HOURS.toMillis(6)));
        Assertions.assertTrue(new BaseCron(ImmutableSet.of()){}.active(TimeUnit.HOURS.toMillis(6)));
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
