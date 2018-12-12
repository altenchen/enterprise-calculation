package storm.tool;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-12-12
 * @description:
 */
@DisplayName("多路延迟开关测试")
final class MultiDelaySwitchTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(MultiDelaySwitchTest.class);

    private MultiDelaySwitchTest() {
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

    @DisplayName("测试状态")
    @RepeatedTest(value = 3, name = "{displayName}: {currentRepetition}/{totalRepetitions}")
    void testIncreaseZero(@NotNull final RepetitionInfo repetitionInfo){
        final Integer switchStatus = repetitionInfo.getCurrentRepetition();

        final Integer thresholdTimes = 1;
        final Long timeoutMillisecond = 0L;
        final Integer nullStatus = null;

        final MultiDelaySwitch<Integer> delaySwitch =
            new MultiDelaySwitch<Integer>()
                .setThresholdTimes(switchStatus, thresholdTimes)
                .setTimeoutMillisecond(switchStatus, timeoutMillisecond)
                .setSwitchStatus(null);
        Assertions.assertEquals(thresholdTimes, delaySwitch.getThresholdTimes(switchStatus));
        Assertions.assertEquals(timeoutMillisecond, delaySwitch.getTimeoutMillisecond(switchStatus));
        Assertions.assertEquals(nullStatus, delaySwitch.getSwitchStatus());

        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];
        delaySwitch.increase(
            switchStatus,
            System.currentTimeMillis(),
            status -> {
                resetCallback[0] += 1;
                Assertions.assertEquals(switchStatus, status);
            },
            (status, threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.assertEquals(switchStatus, status);
                Assertions.assertEquals(thresholdTimes, threshold);
                Assertions.assertEquals(timeoutMillisecond, timeout);
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertEquals(1, overflowCallback[0]);
        Assertions.assertEquals(switchStatus, delaySwitch.getSwitchStatus());

        delaySwitch.increase(
            switchStatus,
            System.currentTimeMillis(),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                Assertions.fail();
            }
        );

    }

    @DisplayName("测试帧数")
    @RepeatedTest(value = 3, name = "{displayName}: {currentRepetition}/{totalRepetitions}")
    void testIncreaseThreshold(@NotNull final RepetitionInfo repetitionInfo) {
        final Integer switchStatus = repetitionInfo.getCurrentRepetition();

        final Integer thresholdTimes = 3;
        final Long timeoutMillisecond = 0L;
        final Integer nullStatus = null;

        final MultiDelaySwitch<Integer> delaySwitch =
            new MultiDelaySwitch<Integer>()
                .setThresholdTimes(switchStatus, thresholdTimes)
                .setTimeoutMillisecond(switchStatus, timeoutMillisecond)
                .setSwitchStatus(null);
        Assertions.assertEquals(thresholdTimes, delaySwitch.getThresholdTimes(switchStatus));
        Assertions.assertEquals(timeoutMillisecond, delaySwitch.getTimeoutMillisecond(switchStatus));
        Assertions.assertEquals(nullStatus, delaySwitch.getSwitchStatus());

        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];

        delaySwitch.increase(
            switchStatus,
            System.currentTimeMillis(),
            status -> {
                resetCallback[0] += 1;
                Assertions.assertEquals(switchStatus, status);
            },
            (status, threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);

        delaySwitch.increase(
            switchStatus,
            System.currentTimeMillis(),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.fail();
            }
        );

        delaySwitch.increase(
            switchStatus,
            System.currentTimeMillis(),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.assertEquals(switchStatus, status);
                Assertions.assertEquals(thresholdTimes, threshold);
                Assertions.assertEquals(timeoutMillisecond, timeout);
            }
        );
        Assertions.assertEquals(1, overflowCallback[0]);

        delaySwitch.increase(
            switchStatus,
            System.currentTimeMillis(),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                Assertions.fail();
            }
        );
    }

    @DisplayName("测试超时")
    @RepeatedTest(value = 3, name = "{displayName}: {currentRepetition}/{totalRepetitions}")
    void testIncreaseTimeout(@NotNull final RepetitionInfo repetitionInfo) {
        final Integer switchStatus = repetitionInfo.getCurrentRepetition();

        final Integer thresholdTimes = 1;
        final Long timeoutMillisecond = TimeUnit.SECONDS.toMillis(30);
        final Integer nullStatus = null;

        final MultiDelaySwitch<Integer> delaySwitch =
            new MultiDelaySwitch<Integer>()
                .setThresholdTimes(switchStatus, thresholdTimes)
                .setTimeoutMillisecond(switchStatus, timeoutMillisecond)
                .setSwitchStatus(null);
        Assertions.assertEquals(thresholdTimes, delaySwitch.getThresholdTimes(switchStatus));
        Assertions.assertEquals(timeoutMillisecond, delaySwitch.getTimeoutMillisecond(switchStatus));
        Assertions.assertEquals(nullStatus, delaySwitch.getSwitchStatus());

        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(0),
            status -> {
                resetCallback[0] += 1;
                Assertions.assertEquals(switchStatus, status);
            },
            (status, threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(15),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.fail();
            }
        );

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(30),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.assertEquals(switchStatus, status);
                Assertions.assertEquals(thresholdTimes, threshold);
                Assertions.assertEquals(timeoutMillisecond, timeout);
            }
        );
        Assertions.assertEquals(1, overflowCallback[0]);

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(45),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                Assertions.fail();
            }
        );
    }

    @DisplayName("测试跳变")
    @RepeatedTest(value = 3, name = "{displayName}: {currentRepetition}/{totalRepetitions}")
    void testIncreaseBreak(@NotNull final RepetitionInfo repetitionInfo) {
        final Integer switchStatus = repetitionInfo.getCurrentRepetition();

        final Integer thresholdTimes = 3;
        final Long timeoutMillisecond = TimeUnit.SECONDS.toMillis(30);
        final Integer nullStatus = null;

        final MultiDelaySwitch<Integer> delaySwitch =
            new MultiDelaySwitch<Integer>()
                .setThresholdTimes(switchStatus, thresholdTimes)
                .setTimeoutMillisecond(switchStatus, timeoutMillisecond)
                .setSwitchStatus(null);
        Assertions.assertEquals(thresholdTimes, delaySwitch.getThresholdTimes(switchStatus));
        Assertions.assertEquals(timeoutMillisecond, delaySwitch.getTimeoutMillisecond(switchStatus));
        Assertions.assertEquals(nullStatus, delaySwitch.getSwitchStatus());

        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(0),
            status -> {
                resetCallback[0] += 1;
                Assertions.assertEquals(switchStatus, status);
            },
            (status, threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.increase(
            nullStatus,
            TimeUnit.SECONDS.toMillis(10),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(20),
            status -> {
                resetCallback[0] += 1;
                Assertions.assertEquals(switchStatus, status);
            },
            (status, threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(2, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(30),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(40),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(50),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.assertEquals(switchStatus, status);
                Assertions.assertEquals(thresholdTimes, threshold);
                Assertions.assertEquals(timeoutMillisecond, timeout);
            }
        );
        Assertions.assertEquals(1, overflowCallback[0]);
        Assertions.assertEquals(switchStatus, delaySwitch.getSwitchStatus());

        delaySwitch.increase(
            switchStatus,
            TimeUnit.SECONDS.toMillis(60),
            status -> {
                Assertions.fail();
            },
            (status, threshold, timeout) -> {
                Assertions.fail();
            }
        );
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
