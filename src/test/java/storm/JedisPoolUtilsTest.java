package storm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import storm.util.JedisPoolUtils;

import java.util.LinkedList;
import java.util.List;

/**
* @author xzp
 * @date: 2018-07-03
 * @description:
*/
@DisplayName("Redis连接池工具测试")
public class JedisPoolUtilsTest {

    @Test
    @DisplayName("测试获取连接池实例")
    public void testGetJedisPool() {
        final JedisPool jedisPool;
        try {
            jedisPool = JedisPoolUtils.getInstance().getJedisPool();
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
        jedis.close();
    }

    @Test
    @DisplayName("测试函数式调用")
    @DisabledOnJre(JRE.OTHER)
    public void testUseResource() {
        try {
            final List<Object> flag = new LinkedList<>();
            JedisPoolUtils.getInstance().useResource(jedis -> {
                Assertions.assertNotNull(jedis, "使用连接失败");
                flag.add(new Object());
            });

            Assertions.assertFalse(() -> flag.isEmpty(), "回调函数没有被调用");

        } catch (Exception e) {
            Assertions.fail("使用连接异常", e);
        }
    }
}
