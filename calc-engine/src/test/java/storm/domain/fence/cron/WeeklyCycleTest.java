package storm.domain.fence.cron;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.DateExtension;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author: xzp
 * @date: 2018-12-01
 * @description:
 */
@DisplayName("每周激活计划测试")
final class WeeklyCycleTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(WeeklyCycleTest.class);

    private WeeklyCycleTest() {
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

    @DisplayName("测试每周激活计划")
    @Test
    void testActive() {

        final long today = DateExtension.getDate(System.currentTimeMillis());
        final int dayOfWeek = DateExtension.getDayOfWeek(today) - 1;

        final long dailyMillisecond = TimeUnit.DAYS.toMillis(1);

        Stream.of(0,1,2,3,4,5,6).forEachOrdered(seed -> {

            // seed % 7 => 0 表示星期天, 1-6 表示星期一到星期六.
            byte weekly_flag = (byte)(1 << ((seed + dayOfWeek) % 7));

            // region prev_day

            {
                final long prev_day = today + dailyMillisecond * ((seed - 1) % 7);

                // region normal_time

                {
                    final long start_time = TimeUnit.HOURS.toMillis(9);
                    final long stop_time = TimeUnit.HOURS.toMillis(15);
                    final WeeklyCycle weeklyCycle = new WeeklyCycle(weekly_flag, start_time, stop_time);

                    Assertions.assertFalse(weeklyCycle.active(prev_day + start_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + start_time));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + start_time + 1));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + stop_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + stop_time));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + stop_time + 1));
                }

                // endregion normal_time

                // region swap_time

                {
                    final long start_time = TimeUnit.HOURS.toMillis(15);
                    final long stop_time = TimeUnit.HOURS.toMillis(9);
                    final WeeklyCycle weeklyCycle = new WeeklyCycle(weekly_flag, start_time, stop_time);

                    Assertions.assertFalse(weeklyCycle.active(prev_day + stop_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + stop_time));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + stop_time + 1));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + start_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + start_time));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + start_time + 1));
                }

                // endregion swap_time

                // region same_time

                {
                    final long same_time = TimeUnit.HOURS.toMillis(12);
                    final WeeklyCycle weeklyCycle = new WeeklyCycle(weekly_flag, same_time, same_time);

                    Assertions.assertFalse(weeklyCycle.active(prev_day + same_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + same_time));
                    Assertions.assertFalse(weeklyCycle.active(prev_day + same_time + 1));
                }

                // endregion same_time
            }

            // endregion prev_day

            // region current_day

            {
                final long day = today + dailyMillisecond * (seed % 7);

                // region normal_time

                {
                    final long start_time = TimeUnit.HOURS.toMillis(9);
                    final long stop_time = TimeUnit.HOURS.toMillis(15);
                    final WeeklyCycle weeklyCycle = new WeeklyCycle(weekly_flag, start_time, stop_time);

                    Assertions.assertFalse(weeklyCycle.active(day + start_time - 1));
                    Assertions.assertTrue(weeklyCycle.active(day + start_time));
                    Assertions.assertTrue(weeklyCycle.active(day + start_time + 1));
                    Assertions.assertTrue(weeklyCycle.active(day + stop_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(day + stop_time));
                    Assertions.assertFalse(weeklyCycle.active(day + stop_time + 1));
                }

                // endregion normal_time

                // region swap_time

                {
                    final long start_time = TimeUnit.HOURS.toMillis(15);
                    final long stop_time = TimeUnit.HOURS.toMillis(9);
                    final WeeklyCycle weeklyCycle = new WeeklyCycle(weekly_flag, start_time, stop_time);

                    Assertions.assertTrue(weeklyCycle.active(day + stop_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(day + stop_time));
                    Assertions.assertFalse(weeklyCycle.active(day + stop_time + 1));
                    Assertions.assertFalse(weeklyCycle.active(day + start_time - 1));
                    Assertions.assertTrue(weeklyCycle.active(day + start_time));
                    Assertions.assertTrue(weeklyCycle.active(day + start_time + 1));
                }

                // endregion swap_time

                // region same_time

                {
                    final long same_time = TimeUnit.HOURS.toMillis(12);
                    final WeeklyCycle weeklyCycle = new WeeklyCycle(weekly_flag, same_time, same_time);

                    Assertions.assertTrue(weeklyCycle.active(day + same_time - 1));
                    Assertions.assertTrue(weeklyCycle.active(day + same_time));
                    Assertions.assertTrue(weeklyCycle.active(day + same_time + 1));
                }

                // endregion same_time
            }

            // endregion current_day

            // region next_day

            {
                final long next_day = today + dailyMillisecond * ((seed + 1) % 7);

                // region normal_time

                {
                    final long start_time = TimeUnit.HOURS.toMillis(9);
                    final long stop_time = TimeUnit.HOURS.toMillis(15);
                    final WeeklyCycle weeklyCycle = new WeeklyCycle(weekly_flag, start_time, stop_time);

                    Assertions.assertFalse(weeklyCycle.active(next_day + start_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(next_day + start_time));
                    Assertions.assertFalse(weeklyCycle.active(next_day + start_time + 1));
                    Assertions.assertFalse(weeklyCycle.active(next_day + stop_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(next_day + stop_time));
                    Assertions.assertFalse(weeklyCycle.active(next_day + stop_time + 1));
                }

                // endregion normal_time

                // region swap_time

                {
                    final long start_time = TimeUnit.HOURS.toMillis(15);
                    final long stop_time = TimeUnit.HOURS.toMillis(9);
                    final WeeklyCycle weeklyCycle = new WeeklyCycle(weekly_flag, start_time, stop_time);

                    Assertions.assertFalse(weeklyCycle.active(next_day + stop_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(next_day + stop_time));
                    Assertions.assertFalse(weeklyCycle.active(next_day + stop_time + 1));
                    Assertions.assertFalse(weeklyCycle.active(next_day + start_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(next_day + start_time));
                    Assertions.assertFalse(weeklyCycle.active(next_day + start_time + 1));
                }

                // endregion swap_time

                // region same_time

                {
                    final long same_time = TimeUnit.HOURS.toMillis(12);
                    final WeeklyCycle weeklyCycle = new WeeklyCycle(weekly_flag, same_time, same_time);

                    Assertions.assertFalse(weeklyCycle.active(next_day + same_time - 1));
                    Assertions.assertFalse(weeklyCycle.active(next_day + same_time));
                    Assertions.assertFalse(weeklyCycle.active(next_day + same_time + 1));
                }

                // endregion same_time
            }

            // endregion next_day
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
