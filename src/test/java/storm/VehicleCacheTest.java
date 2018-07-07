package storm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.util.JedisPoolUtils;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * @author: xzp
 * @date: 2018-07-05
 * @description:
 */
@DisplayName("车辆缓存测试")
public class VehicleCacheTest {

    private static final Logger logger = LoggerFactory.getLogger(VehicleCacheTest.class);

    private static JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();

    private static final VehicleCache CACHE = VehicleCache.getInstance();

    private static final String TEST_VID = "TV-" + UUID.randomUUID();
    private static final String TEST_FIELD_1 = "TF1-" + UUID.randomUUID();
    private static final String TEST_FIELD_2 = "TF2-" + UUID.randomUUID();
    private static final ImmutableSet<String> TEST_FIELD_SET =
        new ImmutableSet.Builder<String>()
            .add(TEST_FIELD_1)
            .add(TEST_FIELD_2)
            .build();
    private static final ImmutableMap<String, String> TEST_MAP =
        new ImmutableMap.Builder<String, String>()
            .put("k1", "v1")
            .put("k2", "v2")
            .put("k3", "v3")
            .build();
    private static final ImmutableMap<String, ImmutableMap<String, String>> TEST_FIELD_MAP =
        new ImmutableMap.Builder<String, ImmutableMap<String, String>>()
            .put(TEST_FIELD_1, TEST_MAP)
            .put(TEST_FIELD_2, TEST_MAP)
            .build();

    private static final int REDIS_DB_INDEX = 6;
    private static final String REDIS_KEY = "vehCache." + TEST_VID;

    private VehicleCacheTest() {
    }

    @SuppressWarnings("unused")
    @BeforeAll
    private static void beforeAll() {
        // 所有测试之前
        CACHE.delFields(TEST_VID);
    }

    @SuppressWarnings("unused")
    @BeforeEach
    private void beforeEach() {
        // 每个测试之前
        CACHE.invalidateAll();
    }

    @DisplayName("测试单个缓存")
    @Test
    void testSingle()
        throws ExecutionException {

        noneExist();

        {
            final ImmutableMap<String, String> dictionary = CACHE.getField(TEST_VID, TEST_FIELD_1);
            Assertions.assertNotNull(dictionary, "缓存不应该为null");
            Assertions.assertTrue(dictionary.isEmpty(), "缓存应该为空");
        }

        CACHE.putField(TEST_VID, TEST_FIELD_1, TEST_MAP);
        onlyField1Exist();

        {
            final ImmutableMap<String, String> dictionary = CACHE.getField(TEST_VID, TEST_FIELD_1);
            Assertions.assertNotNull(dictionary, "缓存不应该为null");
            Assertions.assertFalse(dictionary.isEmpty(), "获取缓存不该为空");
            Assertions.assertEquals(TEST_MAP, dictionary, "缓存内容应该相同");
        }

        CACHE.invalidate(TEST_VID, TEST_FIELD_1);

        {
            final ImmutableMap<String, String> dictionary = CACHE.getField(TEST_VID, TEST_FIELD_1);
            Assertions.assertNotNull(dictionary, "缓存不应该为null");
            Assertions.assertFalse(dictionary.isEmpty(), "缓存不该为空");
            Assertions.assertEquals(TEST_MAP, dictionary, "缓存内容应该相同");
        }

        CACHE.putField(TEST_VID, TEST_FIELD_2, TEST_MAP);
        bothExist();

        CACHE.delField(TEST_VID, TEST_FIELD_1);
        onlyField2Exist();

        CACHE.delField(TEST_VID, TEST_FIELD_2);
        noneExist();

        {
            final ImmutableMap<String, String> dictionary = CACHE.getField(TEST_VID, TEST_FIELD_1);
            Assertions.assertNotNull(dictionary, "缓存不应该为null");
            Assertions.assertTrue(dictionary.isEmpty(), "缓存应该为空");
        }
    }

    @DisplayName("测试批量缓存")
    @Test
    void testMany()
        throws ExecutionException {

        noneExist();

        {
            final Map<String, ImmutableMap<String, String>> dictionaries = CACHE.getFields(TEST_VID, TEST_FIELD_SET);
            Assertions.assertNotNull(dictionaries, "缓存不应该为null");
            for (ImmutableMap<String, String> dictionary : dictionaries.values()) {
                Assertions.assertTrue(dictionary.isEmpty(), "缓存应该为空");
            }
        }

        CACHE.putFields(TEST_VID, TEST_FIELD_MAP);
        bothExist();

        {
            final Map<String, ImmutableMap<String, String>> dictionaries = CACHE.getFields(TEST_VID, TEST_FIELD_SET);
            Assertions.assertNotNull(dictionaries, "缓存不应该为null");
            Assertions.assertEquals(TEST_FIELD_MAP, dictionaries, "缓存内容应该相同");
        }

        CACHE.invalidateAll(TEST_VID, TEST_FIELD_SET);

        {
            final Map<String, ImmutableMap<String, String>> dictionaries = CACHE.getFields(TEST_VID, TEST_FIELD_SET);
            Assertions.assertNotNull(dictionaries, "缓存不应该为null");
            Assertions.assertEquals(TEST_FIELD_MAP, dictionaries, "缓存内容应该相同");
        }

        CACHE.delFields(TEST_VID, TEST_FIELD_SET);
        noneExist();

        {
            final Map<String, ImmutableMap<String, String>> dictionaries = CACHE.getFields(TEST_VID, TEST_FIELD_SET);
            Assertions.assertNotNull(dictionaries, "缓存不应该为null");
            for (ImmutableMap<String, String> dictionary : dictionaries.values()) {
                Assertions.assertTrue(dictionary.isEmpty(), "缓存应该为空");
            }
        }
    }

    private void noneExist() {
        JEDIS_POOL_UTILS.useResource(jedis ->{
            jedis.select(REDIS_DB_INDEX);
            final Set<String> keys = jedis.keys(REDIS_KEY);
            Assertions.assertFalse(keys.contains(REDIS_KEY), "Redis中不应存在测试用vid");
        });
    }

    private void bothExist() {
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            final Set<String> fields = jedis.hkeys(REDIS_KEY);
            Assertions.assertTrue(fields.contains(TEST_FIELD_1), "Redis中应该存在测试用field1");
            Assertions.assertTrue(fields.contains(TEST_FIELD_2), "Redis中应该存在测试用field2");
        });
    }

    private void onlyField1Exist() {
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            final Set<String> fields = jedis.hkeys(REDIS_KEY);
            Assertions.assertTrue(fields.contains(TEST_FIELD_1), "Redis中应该存在测试用field1");
            Assertions.assertFalse(fields.contains(TEST_FIELD_2), "Redis中不应存在测试用field2");
        });
    }

    private void onlyField2Exist() {
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            final Set<String> fields = jedis.hkeys(REDIS_KEY);
            Assertions.assertFalse(fields.contains(TEST_FIELD_1), "Redis中不应存在测试用field1");
            Assertions.assertTrue(fields.contains(TEST_FIELD_2), "Redis中应该存在测试用field2");
        });
    }

    @SuppressWarnings("unused")
    @AfterEach
    private void afterEach() {
        // 每个测试之后
        CACHE.invalidateAll();
    }

    @SuppressWarnings("unused")
    @AfterAll
    private static void afterAll() {
        // 所有测试之后
        CACHE.delFields(TEST_VID);
    }
}
