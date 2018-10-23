package storm.util;

import org.apache.commons.lang.StringUtils;
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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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
    private JedisPool buildJedisPool() {

        logger.info("JedisPool 初始化开始");

        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 可用连接实例的最大数目, 默认为8.
        // 如果赋值为-1, 则表示不限制, 如果pool已经分配了maxActive个jedis实例, 则此时pool的状态为exhausted(耗尽).
        jedisPoolConfig.setMaxTotal(ConfigUtils.getSysDefine().getRedisMaxActive());
        logger.info("{}={}", SysDefine.Redis.JEDIS_POOL_MAX_TOTAL, jedisPoolConfig.getMaxTotal());

        // 控制一个pool最多有多少个状态为idle(空闲)的jedis实例, 默认值是8.
        jedisPoolConfig.setMaxIdle(ConfigUtils.getSysDefine().getRedisMaxIdle());
        logger.info("{}={}", SysDefine.Redis.JEDIS_POOL_MAX_IDLE, jedisPoolConfig.getMaxIdle());

        // 等待可用连接的最大时间, 单位是毫秒, 默认值为-1, 表示永不超时.
        // 如果超过等待时间, 则直接抛出JedisConnectionException
        jedisPoolConfig.setMaxWaitMillis(ConfigUtils.getSysDefine().getRedisMaxWait());
        logger.info("{}={}", SysDefine.Redis.JEDIS_POOL_MAX_WAIT_MILLISECOND, jedisPoolConfig.getMaxWaitMillis());

        // 在borrow(用)一个jedis实例时，是否提前进行validate(验证)操作；
        // 如果为true，则得到的jedis实例均是可用的
        jedisPoolConfig.setTestOnBorrow(true);

        // 在return(还)一个jedis实例时，是否提前进行validate(验证)操作；
        // 如果为true，则返回的jedis实例均是可用的
        jedisPoolConfig.setTestOnReturn(true);

        String password = ConfigUtils.getSysDefine().getRedisPass();
        if(StringUtils.isEmpty(password)){
            password = null;
        }
        if (null == password) {
            logger.info("redis://{}:{}", ConfigUtils.getSysDefine().getRedisHost(), ConfigUtils.getSysDefine().getRedisPort());
        } else {
            logger.info("redis://:{}@{}:{}", ConfigUtils.getSysDefine().getRedisPass(), ConfigUtils.getSysDefine().getRedisHost(), ConfigUtils.getSysDefine().getRedisPort());
        }

        final JedisPool jedisPool = new JedisPool(jedisPoolConfig, ConfigUtils.getSysDefine().getRedisHost(), ConfigUtils.getSysDefine().getRedisPort(), ConfigUtils.getSysDefine().getRedisTimeOut(), password);
        logger.info("JedisPool 初始化完毕");

        return jedisPool;
    }

    {
        JEDIS_POOL = buildJedisPool();
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

    public final void useResource(
        @NotNull final Consumer<? super Jedis> function)
        throws JedisException {

        Objects.requireNonNull(function);

        useResourceInternal(function).accept(JEDIS_POOL);
    }

    public final <A> void useResource(
        @NotNull final BiConsumer<? super Jedis, ? super A> function,
        @Nullable final A argument)
        throws JedisException {

        Objects.requireNonNull(function);

        useResourceInternal(function).accept(JEDIS_POOL, argument);
    }

    public final <R> R useResource(
        @NotNull final Function<? super Jedis, ? extends R> function)
        throws JedisException {

        Objects.requireNonNull(function);

        return useResourceInternal(function).apply(JEDIS_POOL);
    }

    public final <R, D extends R> R useResource(
        @Nullable final D defaultValue,
        @NotNull final BiFunction<? super R, ? super Jedis, D> function)
        throws JedisException {

        Objects.requireNonNull(function);

        return useResourceInternal(function).apply(defaultValue, JEDIS_POOL);
    }

    @NotNull
    @Contract(pure = true)
    private static Consumer<? super JedisPool> useResourceInternal(
        @NotNull final Consumer<? super Jedis> function)
        throws JedisException {

        return jedisPool -> {
            final Jedis jedis = jedisPool.getResource();
            try {
                function.accept(jedis);
                jedisPool.returnResource(jedis);
            } catch (JedisException e) {
                jedisPool.returnBrokenResource(jedis);
                throw e;
            }
        };
    }

    @NotNull
    @Contract(pure = true)
    private static <A> BiConsumer<? super JedisPool, ? super A> useResourceInternal(
        @NotNull final BiConsumer<? super Jedis, ? super A> function)
        throws JedisException {

        return (jedisPool, argument) -> {
            final Jedis jedis = jedisPool.getResource();
            try {
                function.accept(jedis, argument);
                jedisPool.returnResource(jedis);
            } catch (JedisException e) {
                jedisPool.returnBrokenResource(jedis);
                throw e;
            }
        };
    }

    @NotNull
    @Contract(pure = true)
    private static <R> Function<? super JedisPool, ? extends R> useResourceInternal(
        @NotNull final Function<? super Jedis, ? extends R> function)
        throws JedisException {

        return jedisPool -> {
            final Jedis jedis = jedisPool.getResource();
            try {
                final R result = function.apply(jedis);
                jedisPool.returnResource(jedis);
                return result;
            } catch (JedisException e) {
                jedisPool.returnBrokenResource(jedis);
                throw e;
            }
        };
    }

    @NotNull
    @Contract(pure = true)
    private static <R> BiFunction<? super R, ? super JedisPool, ? extends R> useResourceInternal(
        @NotNull final BiFunction<? super R, ? super Jedis, ? extends R> function)
        throws JedisException {

        return (defaultValue, jedisPool) -> {
            final Jedis jedis = jedisPool.getResource();
            try {
                final R result = function.apply(defaultValue, jedis);
                jedisPool.returnResource(jedis);
                return result;
            } catch (JedisException e) {
                jedisPool.returnBrokenResource(jedis);
                throw e;
            }
        };
    }
}
