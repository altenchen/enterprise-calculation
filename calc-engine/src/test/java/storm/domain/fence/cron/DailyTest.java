package storm.domain.fence.cron;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-11-30
 * @description:
 */
@DisplayName("日常激活计划测试")
final class DailyTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DailyTest.class);

    private DailyTest() {
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

    @DisplayName("测试日常激活计划")
    @Test
    void testActive() {

        // region normal_time

        {
            final long start_time = TimeUnit.HOURS.toMillis(9);
            final long stop_time = TimeUnit.HOURS.toMillis(15);
            final Daily daily = new Daily(start_time, stop_time);

            Assertions.assertFalse(daily.active(start_time - 1));
            Assertions.assertTrue(daily.active(start_time));
            Assertions.assertTrue(daily.active(start_time + 1));
            Assertions.assertTrue(daily.active(stop_time - 1));
            Assertions.assertFalse(daily.active(stop_time));
            Assertions.assertFalse(daily.active(stop_time + 1));
        }

        // endregion normal_time

        // region swap_time

        {
            final long start_time = TimeUnit.HOURS.toMillis(15);
            final long stop_time = TimeUnit.HOURS.toMillis(9);
            final Daily daily = new Daily(start_time, stop_time);

            Assertions.assertTrue(daily.active(stop_time - 1));
            Assertions.assertFalse(daily.active(stop_time));
            Assertions.assertFalse(daily.active(stop_time + 1));
            Assertions.assertFalse(daily.active(start_time - 1));
            Assertions.assertTrue(daily.active(start_time));
            Assertions.assertTrue(daily.active(start_time + 1));
        }

        // endregion swap_time

        // region same_time

        {
            final long same_time = TimeUnit.HOURS.toMillis(12);
            final Daily daily = new Daily(same_time, same_time);

            Assertions.assertTrue(daily.active(same_time - 1));
            Assertions.assertTrue(daily.active(same_time));
            Assertions.assertTrue(daily.active(same_time + 1));
        }

        // endregion same_time
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
