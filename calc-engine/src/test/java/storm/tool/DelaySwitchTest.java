package storm.tool;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-10-08
 * @description:
 */
@DisplayName("双路延迟开关单元测试")
public final class DelaySwitchTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DelaySwitchTest.class);

    private DelaySwitchTest() {
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

    @DisplayName("测试正向状态")
    @Test
    void testPositiveIncreaseZero() {
        {
            final DelaySwitch delaySwitch =
                new DelaySwitch(
                    1,
                    0,
                    1,
                    0)
                    .setSwitchStatus(null);

            final byte[] resetCallback = new byte[1];
            final byte[] overflowCallback = new byte[1];
            delaySwitch.positiveIncrease(
                System.currentTimeMillis(),
                () -> {
                    resetCallback[0] += 1;
                },
                (threshold, timeout) -> {
                    overflowCallback[0] += 1;
                    Assertions.assertEquals(Integer.valueOf(1), threshold);
                    Assertions.assertEquals(Long.valueOf(0), timeout);
                }
            );
            Assertions.assertEquals(1, resetCallback[0]);
            Assertions.assertEquals(1, overflowCallback[0]);
            Assertions.assertEquals(Boolean.TRUE, delaySwitch.getSwitchStatus());
        }
        {
            final DelaySwitch delaySwitch =
                new DelaySwitch(
                    1,
                    0,
                    1,
                    0)
                    .setSwitchStatus(false);

            final byte[] resetCallback = new byte[1];
            final byte[] overflowCallback = new byte[1];
            delaySwitch.positiveIncrease(
                System.currentTimeMillis(),
                () -> {
                    resetCallback[0] += 1;
                },
                (threshold, timeout) -> {
                    overflowCallback[0] += 1;
                    Assertions.assertEquals(Integer.valueOf(1), threshold);
                    Assertions.assertEquals(Long.valueOf(0), timeout);
                }
            );
            Assertions.assertEquals(1, resetCallback[0]);
            Assertions.assertEquals(1, overflowCallback[0]);
            Assertions.assertEquals(Boolean.TRUE, delaySwitch.getSwitchStatus());
        }

        {
            final DelaySwitch delaySwitch =
                new DelaySwitch(
                    1,
                    0,
                    1,
                    0)
                    .setSwitchStatus(true);

            delaySwitch.positiveIncrease(
                System.currentTimeMillis(),
                () -> {
                    Assertions.fail();
                },
                (threshold, timeout) -> {
                    Assertions.fail();
                }
            );
        }
    }

    @DisplayName("测试反向状态")
    @Test
    void testNegativeIncreaseZero() {
        {
            final DelaySwitch delaySwitch =
                new DelaySwitch(
                    1,
                    0,
                    1,
                    0)
                    .setSwitchStatus(null);

            final byte[] resetCallback = new byte[1];
            final byte[] overflowCallback = new byte[1];
            delaySwitch.negativeIncrease(
                System.currentTimeMillis(),
                () -> {
                    resetCallback[0] += 1;
                },
                (threshold, timeout) -> {
                    overflowCallback[0] += 1;
                    Assertions.assertEquals(Integer.valueOf(1), threshold);
                    Assertions.assertEquals(Long.valueOf(0), timeout);
                }
            );
            Assertions.assertEquals(1, resetCallback[0]);
            Assertions.assertEquals(1, overflowCallback[0]);
            Assertions.assertEquals(Boolean.FALSE, delaySwitch.getSwitchStatus());
        }

        {
            final DelaySwitch delaySwitch =
                new DelaySwitch(
                    1,
                    0,
                    1,
                    0)
                    .setSwitchStatus(false);

            delaySwitch.negativeIncrease(
                System.currentTimeMillis(),
                () -> {
                    Assertions.fail();
                },
                (threshold, timeout) -> {
                    Assertions.fail();
                }
            );
        }

        {
            final DelaySwitch delaySwitch =
                new DelaySwitch(
                    1,
                    0,
                    1,
                    0)
                    .setSwitchStatus(true);

            final byte[] resetCallback = new byte[1];
            final byte[] overflowCallback = new byte[1];
            delaySwitch.negativeIncrease(
                System.currentTimeMillis(),
                () -> {
                    resetCallback[0] += 1;
                },
                (threshold, timeout) -> {
                    overflowCallback[0] += 1;
                    Assertions.assertEquals(Integer.valueOf(1), threshold);
                    Assertions.assertEquals(Long.valueOf(0), timeout);
                }
            );
            Assertions.assertEquals(1, resetCallback[0]);
            Assertions.assertEquals(1, overflowCallback[0]);
            Assertions.assertEquals(Boolean.FALSE, delaySwitch.getSwitchStatus());
        }
    }

    @DisplayName("测试正向帧数")
    @Test
    void testPositiveIncreaseThreshold() {
        final DelaySwitch delaySwitch =
            new DelaySwitch(
                3,
                0,
                1,
                0)
                .setSwitchStatus(null);
        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];

        delaySwitch.positiveIncrease(
            System.currentTimeMillis(),
            () -> {
                resetCallback[0] += 1;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.positiveIncrease(
            System.currentTimeMillis(),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.positiveIncrease(
            System.currentTimeMillis(),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.assertEquals(Integer.valueOf(3), threshold);
                Assertions.assertEquals(Long.valueOf(0), timeout);
            }
        );
        Assertions.assertEquals(1, overflowCallback[0]);
        Assertions.assertEquals(Boolean.TRUE, delaySwitch.getSwitchStatus());
    }

    @DisplayName("测试反向帧数")
    @Test
    void testNegativeIncreaseThreshold() {
        final DelaySwitch delaySwitch =
            new DelaySwitch(
                1,
                0,
                3,
                0)
                .setSwitchStatus(null);
        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];

        delaySwitch.negativeIncrease(
            System.currentTimeMillis(),
            () -> {
                resetCallback[0] += 1;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.negativeIncrease(
            System.currentTimeMillis(),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.negativeIncrease(
            System.currentTimeMillis(),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.assertEquals(Integer.valueOf(3), threshold);
                Assertions.assertEquals(Long.valueOf(0), timeout);
            }
        );
        Assertions.assertEquals(1, overflowCallback[0]);
        Assertions.assertEquals(Boolean.FALSE, delaySwitch.getSwitchStatus());
    }

    @DisplayName("测试正向超时")
    @Test
    void testPositiveIncreaseTimeout() {
        final DelaySwitch delaySwitch =
            new DelaySwitch(
                1,
                TimeUnit.SECONDS.toMillis(30),
                1,
                0)
                .setSwitchStatus(null);
        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(0),
            () -> {
                resetCallback[0] += 1;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(15),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(30),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.assertEquals(Integer.valueOf(1), threshold);
                Assertions.assertEquals(
                    Long.valueOf(TimeUnit.SECONDS.toMillis(30)),
                    timeout);
            }
        );
        Assertions.assertEquals(1, overflowCallback[0]);
        Assertions.assertEquals(Boolean.TRUE, delaySwitch.getSwitchStatus());

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(45),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
    }

    @DisplayName("测试反向超时")
    @Test
    void testNegativeIncreaseTimeout() {
        final DelaySwitch delaySwitch =
            new DelaySwitch(
                1,
                0,
                1,
                TimeUnit.SECONDS.toMillis(30))
                .setSwitchStatus(null);
        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(0),
            () -> {
                resetCallback[0] += 1;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(15),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(30),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.assertEquals(Integer.valueOf(1), threshold);
                Assertions.assertEquals(
                    Long.valueOf(TimeUnit.SECONDS.toMillis(30)),
                    timeout);
            }
        );
        Assertions.assertEquals(1, overflowCallback[0]);
        Assertions.assertEquals(Boolean.FALSE, delaySwitch.getSwitchStatus());

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(45),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
    }

    @DisplayName("测试正向跳变")
    @Test
    void testPositiveIncreaseBreak() {
        final DelaySwitch delaySwitch =
            new DelaySwitch(
                3,
                TimeUnit.SECONDS.toMillis(30),
                3,
                TimeUnit.SECONDS.toMillis(30))
                .setSwitchStatus(null);
        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(0),
            () -> {
                resetCallback[0] += 1;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(10),
            () -> {
                resetCallback[0] = 0;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(0, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(20),
            () -> {
                resetCallback[0] += 1;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(30),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(40),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(50),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                overflowCallback[0] += 1;
                Assertions.assertEquals(Integer.valueOf(3), threshold);
                Assertions.assertEquals(
                    Long.valueOf(TimeUnit.SECONDS.toMillis(30)),
                    timeout);
            }
        );
        Assertions.assertEquals(1, overflowCallback[0]);
        Assertions.assertEquals(Boolean.TRUE, delaySwitch.getSwitchStatus());

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(60),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
    }

    @DisplayName("测试反向跳变")
    @Test
    void testNegativeIncreaseBreak() {
        final DelaySwitch delaySwitch =
            new DelaySwitch(
                3,
                TimeUnit.SECONDS.toMillis(30),
                3,
                TimeUnit.SECONDS.toMillis(30))
                .setSwitchStatus(null);
        final byte[] resetCallback = new byte[1];
        final byte[] overflowCallback = new byte[1];

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(0),
            () -> {
                resetCallback[0] += 1;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(10),
            () -> {
                resetCallback[0] = 0;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(0, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(20),
            () -> {
                resetCallback[0] += 1;
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );
        Assertions.assertEquals(1, resetCallback[0]);
        Assertions.assertNull(delaySwitch.getSwitchStatus());

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(30),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(40),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.fail();
            }
        );

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(50),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
                Assertions.assertEquals(Integer.valueOf(3), threshold);
                Assertions.assertEquals(
                    Long.valueOf(TimeUnit.SECONDS.toMillis(30)),
                    timeout);
                overflowCallback[0] += 1;
            }
        );
        Assertions.assertEquals(1, overflowCallback[0]);
        Assertions.assertEquals(Boolean.FALSE, delaySwitch.getSwitchStatus());

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(60),
            () -> {
                Assertions.fail();
            },
            (threshold, timeout) -> {
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
