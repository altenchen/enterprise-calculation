package storm;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.handler.ctx.RedisRecorder;
import storm.handler.cusmade.CarLockStatusChangeJudge;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author: xzp
 * @date: 2018-07-12
 * @description:
 */
@DisplayName("RedisRecorderTest测试")
final class RedisRecorderTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(RedisRecorderTest.class);
    private static final RedisRecorder redisRecorder = new RedisRecorder();

    private RedisRecorderTest() {
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

    @Disabled
    @DisplayName("测试重启初始化")
    @Test
    void rebootInit() {

        {
            final int REDIS_DB_INDEX = 6;
            final String REDIS_KEY = "vehCache.qy.idle";
            process(REDIS_DB_INDEX, REDIS_KEY);
        }

        {
            final int REDIS_DB_INDEX = 6;
            final String REDIS_KEY = "vehCache.qy.onoff.notice";
            process(REDIS_DB_INDEX, REDIS_KEY);
        }

        {
            final int REDIS_DB_INDEX = 6;
            final String REDIS_KEY = "vehCache.qy.soc.notice";
            process(REDIS_DB_INDEX, REDIS_KEY);
        }

        {
            final int REDIS_DB_INDEX = 6;
            final String REDIS_KEY = "vehCache.qy.lockStatus.notice";
            process(REDIS_DB_INDEX, REDIS_KEY);
        }
    }

    private void process(
        final int REDIS_DB_INDEX,
        final String REDIS_KEY) {

        final Map<String, Map<String,Object>> storage = new HashMap<>();
        redisRecorder.rebootInit(REDIS_DB_INDEX, REDIS_KEY, storage);

        for (String vid : storage.keySet()) {
            Map<String, Object> dictionary = storage.get(vid);
            for (String key : dictionary.keySet()) {
                final Object value = dictionary.get(key);
                if(Double.class.equals(value.getClass())) {
                    logger.trace("[{}]{}: {}-> \"{}\": {}", REDIS_DB_INDEX, REDIS_KEY, vid, key, value);
                }
            }
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
