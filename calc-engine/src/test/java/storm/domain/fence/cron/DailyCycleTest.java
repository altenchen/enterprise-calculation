package storm.domain.fence.cron;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.DateExtension;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author: xzp
 * @date: 2018-11-30
 * @description:
 */
@DisplayName("每天激活计划测试")
final class DailyCycleTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DailyCycleTest.class);

    private DailyCycleTest() {
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

    @DisplayName("测试每天激活计划")
    @Test
    void testActive() {

        final long today = DateExtension.getDate(System.currentTimeMillis());

        final long dailyMillisecond = TimeUnit.DAYS.toMillis(1);

        Stream.of(0,1,2,3,4,5,6,7).forEachOrdered(seed -> {

            final long day = today + dailyMillisecond * seed;

            // region normal_time

            {
                final long start_time = TimeUnit.HOURS.toMillis(9);
                final long stop_time = TimeUnit.HOURS.toMillis(15);
                final DailyCycle dailyCycle = new DailyCycle(start_time, stop_time);

                Assertions.assertFalse(dailyCycle.active(day + start_time - 1));
                Assertions.assertTrue(dailyCycle.active(day + start_time));
                Assertions.assertTrue(dailyCycle.active(day + start_time + 1));
                Assertions.assertTrue(dailyCycle.active(day + stop_time - 1));
                Assertions.assertFalse(dailyCycle.active(day + stop_time));
                Assertions.assertFalse(dailyCycle.active(day + stop_time + 1));
            }

            // endregion normal_time

            // region swap_time

            {
                final long start_time = TimeUnit.HOURS.toMillis(15);
                final long stop_time = TimeUnit.HOURS.toMillis(9);
                final DailyCycle dailyCycle = new DailyCycle(start_time, stop_time);

                Assertions.assertTrue(dailyCycle.active(day + stop_time - 1));
                Assertions.assertFalse(dailyCycle.active(day + stop_time));
                Assertions.assertFalse(dailyCycle.active(day + stop_time + 1));
                Assertions.assertFalse(dailyCycle.active(day + start_time - 1));
                Assertions.assertTrue(dailyCycle.active(day + start_time));
                Assertions.assertTrue(dailyCycle.active(day + start_time + 1));
            }

            // endregion swap_time

            // region same_time

            {
                final long same_time = TimeUnit.HOURS.toMillis(12);
                final DailyCycle dailyCycle = new DailyCycle(same_time, same_time);

                Assertions.assertTrue(dailyCycle.active(day + same_time - 1));
                Assertions.assertTrue(dailyCycle.active(day + same_time));
                Assertions.assertTrue(dailyCycle.active(day + same_time + 1));
            }

            // endregion same_time
        });
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
