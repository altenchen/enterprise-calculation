package storm.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;
import storm.system.SysDefine;

import java.util.Objects;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Redis 连接池工具
 *
 * @author xzp
 */
@SuppressWarnings("unused")
public final class JedisPoolUtils {

    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(JedisPoolUtils.class);

    @NotNull
    private static final JedisPoolUtils INSTANCE = new JedisPoolUtils();

    @Contract(pure = true)
    public static JedisPoolUtils getInstance() {
        return INSTANCE;
    }

    @NotNull
    private final JedisPool JEDIS_POOL;

    @NotNull
    private JedisPool buildJedisPool(@NotNull final Properties sysDefine) {

        logger.info("JedisPool 初始化开始");

        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        final String maxTotalString = sysDefine.getProperty(SysDefine.Redis.JEDIS_POOL_MAX_TOTAL);
        if (NumberUtils.isNumber(maxTotalString)) {
            final int maxTotal = Integer.parseInt(maxTotalString);
            // 可用连接实例的最大数目, 默认为8.
            // 如果赋值为-1, 则表示不限制, 如果pool已经分配了maxActive个jedis实例, 则此时pool的状态为exhausted(耗尽).
            jedisPoolConfig.setMaxTotal(maxTotal);
        }
        logger.info("{}={}", SysDefine.Redis.JEDIS_POOL_MAX_TOTAL, jedisPoolConfig.getMaxTotal());

        final String maxIdleString = sysDefine.getProperty(SysDefine.Redis.JEDIS_POOL_MAX_IDLE);
        final int maxIdle;
        if (NumberUtils.isNumber(maxIdleString)) {
            maxIdle = Integer.parseInt(maxIdleString);
            // 控制一个pool最多有多少个状态为idle(空闲)的jedis实例, 默认值是8.
            jedisPoolConfig.setMaxIdle(maxIdle);
        }
        logger.info("{}={}", SysDefine.Redis.JEDIS_POOL_MAX_IDLE, jedisPoolConfig.getMaxIdle());

        final String maxWaitMillisString = sysDefine.getProperty(SysDefine.Redis.JEDIS_POOL_MAX_WAIT_MILLISECOND);
        final int maxWaitMillis;
        if (NumberUtils.isNumber(maxWaitMillisString)) {
            maxWaitMillis = Integer.parseInt(maxWaitMillisString);
            // 等待可用连接的最大时间, 单位是毫秒, 默认值为-1, 表示永不超时.
            // 如果超过等待时间, 则直接抛出JedisConnectionException
            jedisPoolConfig.setMaxWaitMillis(maxWaitMillis);
        }
        logger.info("{}={}", SysDefine.Redis.JEDIS_POOL_MAX_WAIT_MILLISECOND, jedisPoolConfig.getMaxWaitMillis());

        // 在borrow(用)一个jedis实例时，是否提前进行validate(验证)操作；
        // 如果为true，则得到的jedis实例均是可用的
        jedisPoolConfig.setTestOnBorrow(true);

        // 在return(还)一个jedis实例时，是否提前进行validate(验证)操作；
        // 如果为true，则返回的jedis实例均是可用的
        jedisPoolConfig.setTestOnReturn(true);

        // 主机地址
        final String host = sysDefine.getProperty(SysDefine.Redis.HOST, "localhost");

        // 端口号
        final String portString = sysDefine.getProperty(SysDefine.Redis.PORT);
        final int port;
        if (NumberUtils.isNumber(portString)) {
            port = Integer.parseInt(portString);
        } else {
            port = 6379;
        }

        // 密码
        final String password = StringUtils.defaultIfEmpty(sysDefine.getProperty(SysDefine.Redis.PASSWORD), null);

        // 超时时间
        final String timeOutString = sysDefine.getProperty(SysDefine.Redis.TIMEOUT);
        final int timeout;
        if (NumberUtils.isNumber(timeOutString)) {
            timeout = Integer.parseInt(timeOutString);
        } else {
            timeout = 2000;
        }

        if (null == password) {
            logger.info("redis://{}:{}", host, port);
        } else {
            logger.info("redis://:{}@{}:{}", password, host, port);
        }

        final JedisPool jedisPool = new JedisPool(jedisPoolConfig, host, port, timeout, password);

        logger.info("JedisPool 初始化完毕");

        return jedisPool;
    }

    {
        final ConfigUtils configUtils = ConfigUtils.getInstance();
        JEDIS_POOL = buildJedisPool(configUtils.sysDefine);
    }

    private JedisPoolUtils() {
    }

    /**
     * 建议使用 useResource 方法
     */
    @Contract(pure = true)
    public final JedisPool getJedisPool() {
        return JEDIS_POOL;
    }

    @Nullable
    public final <R> R useResource(
        @NotNull final Function<? super Jedis, ? extends R> action)
        throws JedisException {

        Objects.requireNonNull(action);

        return useResourceInternal(action).apply(JEDIS_POOL);
    }

    public final <R> R useResource(
        @Nullable final R defaultValue,
        @NotNull final BiFunction<R, Jedis, R> action)
        throws JedisException {

        Objects.requireNonNull(action);

        return useResourceInternal(action).apply(defaultValue, JEDIS_POOL);
    }

    @NotNull
    @Contract(pure = true)
    private static <R> Function<? super JedisPool, ? extends R> useResourceInternal(
        @NotNull final Function<? super Jedis, ? extends R> action)
        throws JedisException {

        return jedisPool -> {
            final Jedis jedis = jedisPool.getResource();
            try {
                return action.apply(jedis);
            } catch (JedisException e) {
                jedisPool.returnBrokenResource(jedis);
                throw e;
            } finally {
                jedisPool.returnResource(jedis);
            }
        };
    }

    @NotNull
    @Contract(pure = true)
    private static <R> BiFunction<R, JedisPool, R> useResourceInternal(
        @NotNull final BiFunction<R, Jedis, R> action)
        throws JedisException {

        return (defaultValue, jedisPool) -> {
            final Jedis jedis = jedisPool.getResource();
            try {
                final R result = action.apply(defaultValue, jedis);
                jedisPool.returnResource(jedis);
                return result;
            } catch (JedisException e) {
                jedisPool.returnBrokenResource(jedis);
                throw e;
            }
        };
    }
}
