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
@DisplayName("延迟开关单元测试")
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
            final DelaySwitch delaySwitch = new DelaySwitch(
                1,
                0,
                1,
                0);
            final byte[] flag = new byte[2];
            delaySwitch.setSwitchStatus(null);
            delaySwitch.positiveIncrease(
                System.currentTimeMillis(),
                () -> {
                    flag[0] += 1;
                },
                (positiveThreshold, positiveTimeout) -> {
                    flag[1] += 1;
                }
            );
            Assertions.assertEquals(1, flag[0]);
            Assertions.assertEquals(1, flag[1]);
        }
        {
            final DelaySwitch delaySwitch = new DelaySwitch(
                1,
                0,
                1,
                0);
            final byte[] flag = new byte[2];
            delaySwitch.setSwitchStatus(false);
            delaySwitch.positiveIncrease(
                System.currentTimeMillis(),
                () -> {
                    flag[0] += 1;
                },
                (positiveThreshold, positiveTimeout) -> {
                    flag[1] += 1;
                }
            );
            Assertions.assertEquals(1, flag[0]);
            Assertions.assertEquals(1, flag[1]);
        }

        {
            final DelaySwitch delaySwitch = new DelaySwitch(
                1,
                0,
                1,
                0);
            final byte[] flag = new byte[2];
            delaySwitch.setSwitchStatus(true);
            delaySwitch.positiveIncrease(
                System.currentTimeMillis(),
                () -> {
                    flag[0] += 1;
                },
                (positiveThreshold, positiveTimeout) -> {
                    flag[1] += 1;
                }
            );
            Assertions.assertEquals(0, flag[0]);
            Assertions.assertEquals(0, flag[1]);
        }
    }

    @DisplayName("测试反向状态")
    @Test
    void testNegativeIncreaseZero() {
        {
            final DelaySwitch delaySwitch = new DelaySwitch(
                1,
                0,
                1,
                0);
            final byte[] flag = new byte[2];
            delaySwitch.setSwitchStatus(null);
            delaySwitch.negativeIncrease(
                System.currentTimeMillis(),
                () -> {
                    flag[0] += 1;
                },
                (positiveThreshold, positiveTimeout) -> {
                    flag[1] += 1;
                }
            );
            Assertions.assertEquals(1, flag[0]);
            Assertions.assertEquals(1, flag[1]);
        }

        {
            final DelaySwitch delaySwitch = new DelaySwitch(
                1,
                0,
                1,
                0);
            final byte[] flag = new byte[2];
            delaySwitch.setSwitchStatus(false);
            delaySwitch.negativeIncrease(
                System.currentTimeMillis(),
                () -> {
                    flag[0] += 1;
                },
                (positiveThreshold, positiveTimeout) -> {
                    flag[1] += 1;
                }
            );
            Assertions.assertEquals(0, flag[0]);
            Assertions.assertEquals(0, flag[1]);
        }

        {
            final DelaySwitch delaySwitch = new DelaySwitch(
                1,
                0,
                1,
                0);
            final byte[] flag = new byte[2];
            delaySwitch.setSwitchStatus(true);
            delaySwitch.negativeIncrease(
                System.currentTimeMillis(),
                () -> {
                    flag[0] += 1;
                },
                (positiveThreshold, positiveTimeout) -> {
                    flag[1] += 1;
                }
            );
            Assertions.assertEquals(1, flag[0]);
            Assertions.assertEquals(1, flag[1]);
        }
    }

    @DisplayName("测试正向帧数")
    @Test
    void testPositiveIncreaseThreshold() {
        final DelaySwitch delaySwitch = new DelaySwitch(
            3,
            0,
            1,
            0);
        final byte[] flag = new byte[2];

        delaySwitch.positiveIncrease(
            System.currentTimeMillis(),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.positiveIncrease(
            System.currentTimeMillis(),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.positiveIncrease(
            System.currentTimeMillis(),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(1, flag[1]);
    }

    @DisplayName("测试反向帧数")
    @Test
    void testNegativeIncreaseThreshold() {
        final DelaySwitch delaySwitch = new DelaySwitch(
            1,
            0,
            3,
            0);
        final byte[] flag = new byte[2];

        delaySwitch.negativeIncrease(
            System.currentTimeMillis(),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.negativeIncrease(
            System.currentTimeMillis(),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.negativeIncrease(
            System.currentTimeMillis(),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(1, flag[1]);
    }

    @DisplayName("测试正向超时")
    @Test
    void testPositiveIncreaseTimeout() {
        final DelaySwitch delaySwitch = new DelaySwitch(
            1,
            TimeUnit.SECONDS.toMillis(30),
            1,
            0);
        final byte[] flag = new byte[2];

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(0),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(15),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(30),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(1, flag[1]);
    }

    @DisplayName("测试反向超时")
    @Test
    void testNegativeIncreaseTimeout() {
        final DelaySwitch delaySwitch = new DelaySwitch(
            1,
            0,
            1,
            TimeUnit.SECONDS.toMillis(30));
        final byte[] flag = new byte[2];

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(0),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(15),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(30),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(1, flag[1]);
    }

    @DisplayName("测试正向跳变")
    @Test
    void testPositiveIncreaseBreak() {
        final DelaySwitch delaySwitch = new DelaySwitch(
            3,
            TimeUnit.SECONDS.toMillis(30),
            3,
            TimeUnit.SECONDS.toMillis(30));
        final byte[] flag = new byte[4];

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(0),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(10),
            () -> {
                flag[2] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[3] += 1;
            }
        );
        Assertions.assertEquals(1, flag[2]);
        Assertions.assertEquals(0, flag[3]);

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(20),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(2, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(30),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(2, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(40),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(2, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(50),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(2, flag[0]);
        Assertions.assertEquals(1, flag[1]);
    }

    @DisplayName("测试反向跳变")
    @Test
    void testNegativeIncreaseBreak() {
        final DelaySwitch delaySwitch = new DelaySwitch(
            3,
            TimeUnit.SECONDS.toMillis(30),
            3,
            TimeUnit.SECONDS.toMillis(30));
        final byte[] flag = new byte[4];

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(0),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(1, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.positiveIncrease(
            TimeUnit.SECONDS.toMillis(10),
            () -> {
                flag[2] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[3] += 1;
            }
        );
        Assertions.assertEquals(1, flag[2]);
        Assertions.assertEquals(0, flag[3]);

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(20),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(2, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(30),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(2, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(40),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(2, flag[0]);
        Assertions.assertEquals(0, flag[1]);

        delaySwitch.negativeIncrease(
            TimeUnit.SECONDS.toMillis(50),
            () -> {
                flag[0] += 1;
            },
            (positiveThreshold, positiveTimeout) -> {
                flag[1] += 1;
            }
        );
        Assertions.assertEquals(2, flag[0]);
        Assertions.assertEquals(1, flag[1]);
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
