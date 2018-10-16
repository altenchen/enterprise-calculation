package storm.extension;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * @author: xzp
 * @date: 2018-10-16
 * @description:
 */
@DisplayName("MapExtensionTest")
public final class MapExtensionTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(MapExtensionTest.class);

    private MapExtensionTest() {
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

    @DisplayName("测试方法")
    @Test
    void testMethod() {

        final HashMap<String, String> nullMap = Maps.newHashMap();
        nullMap.put(null, "value");
        MapExtension.clearNullEntry(nullMap);
        Assertions.assertEquals(0, nullMap.size());
        nullMap.put("key", null);
        MapExtension.clearNullEntry(nullMap);
        Assertions.assertEquals(0, nullMap.size());
        nullMap.put("key", "value");
        Assertions.assertEquals(1, nullMap.size());
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
