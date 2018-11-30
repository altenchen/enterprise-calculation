package storm.domain.fence.cron;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.DateExtension;

import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-11-30
 * @description:
 */
@DisplayName("单次激活计划测试")
final class DailyOnceTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DailyOnceTest.class);

    private DailyOnceTest() {
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

    @DisplayName("测试单次激活计划")
    @Test
    void testActive() {
        final long today = DateExtension.getDate(System.currentTimeMillis());

        final long dailyMillisecond = TimeUnit.DAYS.toMillis(1);


        // region normal_day

        {
            final long start_day = today - dailyMillisecond;
            final long stop_day = today + dailyMillisecond;

            // region normal_time

            {
                final long start_time = TimeUnit.HOURS.toMillis(9);
                final long stop_time = TimeUnit.HOURS.toMillis(15);
                final DailyOnce dailyOnce = new DailyOnce(start_day, stop_day, start_time, stop_time);

                {
                    long day = start_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time));
                    Assertions.assertFalse(dailyOnce.active(day + start_time + 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(start_day + start_time - 1));
                    Assertions.assertTrue(dailyOnce.active(start_day + start_time));
                    Assertions.assertTrue(dailyOnce.active(start_day + start_time + 1));
                    Assertions.assertTrue(dailyOnce.active(start_day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time + 1));
                }

                {
                    long day = stop_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertTrue(dailyOnce.active(day + start_time));
                    Assertions.assertTrue(dailyOnce.active(day + start_time + 1));
                    Assertions.assertTrue(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time + 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time + 1));
                }
            }

            // endregion normal_time

            // region swap_time

            {
                final long start_time = TimeUnit.HOURS.toMillis(15);
                final long stop_time = TimeUnit.HOURS.toMillis(9);
                final DailyOnce dailyOnce = new DailyOnce(start_day, stop_day, start_time, stop_time);

                {
                    long day = start_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time));
                    Assertions.assertFalse(dailyOnce.active(day + start_time + 1));
                }

                {
                    Assertions.assertTrue(dailyOnce.active(start_day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + start_time - 1));
                    Assertions.assertTrue(dailyOnce.active(start_day + start_time));
                    Assertions.assertTrue(dailyOnce.active(start_day + start_time + 1));
                }

                {
                    long day = stop_day - dailyMillisecond;

                    Assertions.assertTrue(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertTrue(dailyOnce.active(day + start_time));
                    Assertions.assertTrue(dailyOnce.active(day + start_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time + 1));
                }
            }

            // endregion swap_time

            // region same_time

            {
                final long same_time = TimeUnit.HOURS.toMillis(12);
                final DailyOnce dailyOnce = new DailyOnce(start_day, stop_day, same_time, same_time);

                {
                    long day = start_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + same_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + same_time));
                    Assertions.assertFalse(dailyOnce.active(day + same_time + 1));
                }

                {
                    Assertions.assertTrue(dailyOnce.active(start_day + same_time - 1));
                    Assertions.assertTrue(dailyOnce.active(start_day + same_time));
                    Assertions.assertTrue(dailyOnce.active(start_day + same_time + 1));
                }

                {
                    long day = stop_day - dailyMillisecond;

                    Assertions.assertTrue(dailyOnce.active(day + same_time - 1));
                    Assertions.assertTrue(dailyOnce.active(day + same_time));
                    Assertions.assertTrue(dailyOnce.active(day + same_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(stop_day + same_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + same_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + same_time + 1));
                }
            }

            // endregion same_time
        }

        // endregion normal_day

        // region swap_day

        {
            final long start_day = today + dailyMillisecond;
            final long stop_day = today - dailyMillisecond;

            // region normal_time

            {
                final long start_time = TimeUnit.HOURS.toMillis(9);
                final long stop_time = TimeUnit.HOURS.toMillis(15);
                final DailyOnce dailyOnce = new DailyOnce(start_day, stop_day, start_time, stop_time);

                {
                    long day = start_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time));
                    Assertions.assertFalse(dailyOnce.active(day + start_time + 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(start_day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + start_time));
                    Assertions.assertFalse(dailyOnce.active(start_day + start_time + 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time + 1));
                }

                {
                    long day = stop_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time));
                    Assertions.assertFalse(dailyOnce.active(day + start_time + 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time + 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time + 1));
                }
            }

            // endregion normal_time

            // region swap_time

            {
                final long start_time = TimeUnit.HOURS.toMillis(15);
                final long stop_time = TimeUnit.HOURS.toMillis(9);
                final DailyOnce dailyOnce = new DailyOnce(start_day, stop_day, start_time, stop_time);

                {
                    long day = start_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time));
                    Assertions.assertFalse(dailyOnce.active(day + start_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(start_day + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + start_time));
                    Assertions.assertFalse(dailyOnce.active(start_day + start_time + 1));
                }

                {
                    long day = stop_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time));
                    Assertions.assertFalse(dailyOnce.active(day + start_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + start_time + 1));
                }
            }

            // endregion swap_time

            // region same_time

            {
                final long same_time = TimeUnit.HOURS.toMillis(12);
                final DailyOnce dailyOnce = new DailyOnce(start_day, stop_day, same_time, same_time);

                {
                    long day = start_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + same_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + same_time));
                    Assertions.assertFalse(dailyOnce.active(day + same_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(start_day + same_time - 1));
                    Assertions.assertFalse(dailyOnce.active(start_day + same_time));
                    Assertions.assertFalse(dailyOnce.active(start_day + same_time + 1));
                }

                {
                    long day = stop_day - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + same_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + same_time));
                    Assertions.assertFalse(dailyOnce.active(day + same_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(stop_day + same_time - 1));
                    Assertions.assertFalse(dailyOnce.active(stop_day + same_time));
                    Assertions.assertFalse(dailyOnce.active(stop_day + same_time + 1));
                }
            }

            // endregion same_time
        }

        // endregion swap_day

        // region same_day

        {

            // region normal_time

            {
                final long start_time = TimeUnit.HOURS.toMillis(9);
                final long stop_time = TimeUnit.HOURS.toMillis(15);
                final DailyOnce dailyOnce = new DailyOnce(today, today, start_time, stop_time);

                {
                    long day = today - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time));
                    Assertions.assertFalse(dailyOnce.active(day + start_time + 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(today + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(today + start_time));
                    Assertions.assertFalse(dailyOnce.active(today + start_time + 1));
                    Assertions.assertFalse(dailyOnce.active(today + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(today + stop_time));
                    Assertions.assertFalse(dailyOnce.active(today + stop_time + 1));
                }
            }

            // endregion normal_time

            // region swap_time

            {
                final long start_time = TimeUnit.HOURS.toMillis(15);
                final long stop_time = TimeUnit.HOURS.toMillis(9);
                final DailyOnce dailyOnce = new DailyOnce(today, today, start_time, stop_time);

                {
                    long day = today - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time));
                    Assertions.assertFalse(dailyOnce.active(day + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + start_time));
                    Assertions.assertFalse(dailyOnce.active(day + start_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(today + stop_time - 1));
                    Assertions.assertFalse(dailyOnce.active(today + stop_time));
                    Assertions.assertFalse(dailyOnce.active(today + stop_time + 1));
                    Assertions.assertFalse(dailyOnce.active(today + start_time - 1));
                    Assertions.assertFalse(dailyOnce.active(today + start_time));
                    Assertions.assertFalse(dailyOnce.active(today + start_time + 1));
                }
            }

            // endregion swap_time

            // region same_time

            {
                final long same_time = TimeUnit.HOURS.toMillis(12);
                final DailyOnce dailyOnce = new DailyOnce(today, today, same_time, same_time);

                {
                    long day = today - dailyMillisecond;

                    Assertions.assertFalse(dailyOnce.active(day + same_time - 1));
                    Assertions.assertFalse(dailyOnce.active(day + same_time));
                    Assertions.assertFalse(dailyOnce.active(day + same_time + 1));
                }

                {
                    Assertions.assertFalse(dailyOnce.active(today + same_time - 1));
                    Assertions.assertFalse(dailyOnce.active(today + same_time));
                    Assertions.assertFalse(dailyOnce.active(today + same_time + 1));
                }
            }

            // endregion same_time
        }

        // endregion same_day
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
