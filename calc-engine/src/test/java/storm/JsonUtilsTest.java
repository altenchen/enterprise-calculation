package storm;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.alarm.CoefficientOffset;
import storm.util.JsonUtils;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author: xzp
 * @date: 2018-07-04
 * @description: Gson工具类测试
 */
@DisplayName("Gson工具测试")
final class JsonUtilsTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(JsonUtilsTest.class);

    private static final JsonUtils GSON_UTILS = JsonUtils.getInstance();

    private JsonUtilsTest() {
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

    @DisplayName("测试序列化")
    @Test
    void toJson() {
        Map<String, Object> dic = new HashMap<>();
        dic.put("k1", "v1");
        dic.put("k2", "v2");
        dic.put("k3", "v3");
        dic.put("Date", new Date());
        dic.put("NULL", null);
        final String json = GSON_UTILS.toJson(dic);
        logger.trace(json);

        Assertions.assertFalse(StringUtils.isBlank(json), "Json序列化不能为空白");
    }

    @DisplayName("测试反序列化")
    @Test
    void fromJson() {
        final String stringKey = "stringKey";
        final String stringValue = "stringValue";
        final String booleanKey = "booleanKey";
        final boolean booleanValue = true;
        final String floatKey = "floatKey";
        final float floatValue = 2.22F;
        final String intKey = "intKey";
        final int intValue = 111;

        final String stringJson = GSON_UTILS.toJson(stringValue);
        Assertions.assertEquals(String.class, GSON_UTILS.fromJson(stringJson, Object.class).getClass());

        final String booleanJson = GSON_UTILS.toJson(booleanValue);
        Assertions.assertEquals(Boolean.class, GSON_UTILS.fromJson(booleanJson, Object.class).getClass());

        final String floatJson = GSON_UTILS.toJson(floatValue);
        Assertions.assertEquals(BigDecimal.class, GSON_UTILS.fromJson(floatJson, Object.class).getClass());

        final String intJson = GSON_UTILS.toJson(intValue);
        Assertions.assertEquals(Integer.class, GSON_UTILS.fromJson(intJson, Integer.class).getClass());

        final ImmutableMap<String, Object> srcMap = new ImmutableMap.Builder<String, Object>()
            .put(stringKey, stringValue)
            .put(booleanKey, booleanValue)
            .put(floatKey, floatValue)
            .put(intKey, intValue)
            .build();

        final String mapToJson = GSON_UTILS.toJson(srcMap);

        final TreeMap<String, Object> treeMap = GSON_UTILS.fromJson(
            mapToJson,
            new TypeToken<TreeMap<String, Object>>() {
            }.getType());

        Assertions.assertEquals(String.class, treeMap.get(stringKey).getClass());
        Assertions.assertEquals(Boolean.class, treeMap.get(booleanKey).getClass());
        Assertions.assertEquals(BigDecimal.class, treeMap.get(floatKey).getClass());
        Assertions.assertEquals(Integer.class, treeMap.get(intKey).getClass());

        final HashMap<String, Object> hashMap = GSON_UTILS.fromJson(
            mapToJson,
            new TypeToken<HashMap<String, Object>>() {
            }.getType());

        Assertions.assertEquals(String.class, hashMap.get(stringKey).getClass());
        Assertions.assertEquals(Boolean.class, hashMap.get(booleanKey).getClass());
        Assertions.assertEquals(BigDecimal.class, hashMap.get(floatKey).getClass());
        Assertions.assertEquals(Integer.class, hashMap.get(intKey).getClass());

        final Map<String, Object> defaultMap = GSON_UTILS.fromJson(
            mapToJson,
            new TypeToken<Map<String, Object>>() {
            }.getType());

        Assertions.assertEquals(String.class, defaultMap.get(stringKey).getClass());
        Assertions.assertEquals(Boolean.class, defaultMap.get(booleanKey).getClass());
        Assertions.assertEquals(BigDecimal.class, defaultMap.get(floatKey).getClass());
        Assertions.assertEquals(Integer.class, defaultMap.get(intKey).getClass());


        final ImmutableSet<Object> srcSet = new ImmutableSet.Builder<>()
            .add(intValue + 1)
            .add(intValue + 2)
            .add(intValue + 3)
            .build();
        final String setToJson = GSON_UTILS.toJson(srcSet);

        final List<Object> defaultList = GSON_UTILS.fromJson(
            setToJson,
            new TypeToken<List<Object>>() {
            }.getType());

        for (Object item : defaultList) {
            Assertions.assertEquals(Integer.class, item.getClass());
        }

        final Object[] defaultArray = GSON_UTILS.fromJson(
            setToJson,
            new TypeToken<Object[]>() {
            }.getType());

        for (Object item : defaultArray) {
            Assertions.assertEquals(Integer.class, item.getClass());
        }
    }

    @DisplayName("测试反序列化2")
    @Test
    void fromJson2() {


        final String json = "{\"mileage\":-1,\"msgId\":\"08d6d6e8d6db41f5bc15043d8fdae8f5\",\"msgType\":\"IDLE_VEH\",\"noticetime\":\"20180614103052\",\"soc\":-1,\"speed\":-1,\"status\":1,\"stime\":\"20180614102755\",\"vid\":\"b3f4fc61-8cfd-4561-815a-7bb46fde6bec\"}";

        final Map<String, Object> defaultMap2 = GSON_UTILS.fromJson(
            json,
            new TypeToken<Map<String, Object>>() {
            }.getType());
        for (final String key : defaultMap2.keySet()) {
            final Object value = defaultMap2.get(key);
            if(Double.class.equals(value.getClass())) {
                logger.trace("\"{}\": {}", key, value);
            } else {
                logger.trace("{} -> {}", value, value.getClass());
            }
            Assertions.assertNotEquals(Double.class, value.getClass());
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
