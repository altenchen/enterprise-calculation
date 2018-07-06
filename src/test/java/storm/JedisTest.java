package storm;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisDataException;
import storm.constant.RedisConstant;
import storm.util.JedisPoolUtils;

/**
 * @author: xzp
 * @date: 2018-07-05
 * @description:
 */
@DisplayName("Jedis试验")
final class JedisTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(JedisTest.class);

    private JedisTest() {}

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
    void testMethod() {
        final JedisPoolUtils jedisPoolUtils = JedisPoolUtils.getInstance();

        final String select = jedisPoolUtils.useResource(jedis -> {
            return jedis.select(0);
        });
        Assertions.assertEquals(RedisConstant.Select.OK.name(), select);

        final JedisDataException exception = jedisPoolUtils.useResource(jedis -> {
            try {
                jedis.select(-1);
                Assertions.fail();
                return null;
            } catch (JedisDataException e) {
                return e;
            }
        });
        Assertions.assertNotNull(exception);
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
