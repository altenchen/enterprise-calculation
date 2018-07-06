package storm;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import storm.util.JedisPoolUtils;

/**
 * @author xzp
 * @date: 2018-07-03
 * @description:
 */
@DisplayName("Redis连接池工具测试")
final class JedisPoolUtilsTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(JedisPoolUtilsTest.class);

    private JedisPoolUtilsTest() {}

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

    @DisplayName("测试获取连接池单例")
    @Test
    void getInstance() {
        final JedisPoolUtils instance1 = JedisPoolUtils.getInstance();
        Assertions.assertNotNull(instance1, () -> "获取" + JedisPool.class.getName() + "实例失败");

        final JedisPoolUtils instance2 = JedisPoolUtils.getInstance();
        Assertions.assertNotNull(instance2, () -> "获取" + JedisPool.class.getName() + "实例失败");

        Assertions.assertSame(instance1, instance2, "获取的单例不同");
    }

    @DisplayName("测试获取连接池实例和连接实例")
    @Test
    void getJedisPool() {
        final JedisPoolUtils instance = JedisPoolUtils.getInstance();
        final JedisPool jedisPool;
        try {
            jedisPool = instance.getJedisPool();
        } catch (Exception e) {
            Assertions.fail("获取连接池异常", e);
            return;
        }
        Assertions.assertNotNull(jedisPool, "获取连接池失败");

        final Jedis jedis;
        try {
            jedis = jedisPool.getResource();
        } catch (Exception e) {
            Assertions.fail("获取连接异常", e);
            return;
        }

        Assertions.assertNotNull(jedis, "获取连接失败");
        jedisPool.returnResourceObject(jedis);
    }

    @DisplayName("测试函数式调用")
    @DisabledOnJre(JRE.OTHER)
    @Test
    void useResource() {
        final JedisPoolUtils instance = JedisPoolUtils.getInstance();
        try {
            final Object object = instance.useResource(
                jedis -> {
                    Assertions.assertNotNull(jedis, "使用连接失败");
                    return new Object();
                });

            Assertions.assertNotNull(object, "回调函数没有被调用");

        } catch (Exception e) {
            Assertions.fail("使用连接异常", e);
        }
    }

    @DisplayName("测试函数式调用")
    @DisabledOnJre(JRE.OTHER)
    @Test
    void useResourceWithDefault() {
        final JedisPoolUtils instance = JedisPoolUtils.getInstance();
        try {

            final Object object = instance.useResource(
                new Object(),
                (defaultValue, jedis) -> {
                    Assertions.assertNotNull(jedis, "使用连接失败");
                    return defaultValue;
                });

            Assertions.assertNotNull(object, "回调函数没有被调用");

        } catch (Exception e) {
            Assertions.fail("使用连接异常", e);
        }
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
