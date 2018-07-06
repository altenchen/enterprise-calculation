package storm;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author: xzp
 * @date: 2018-07-05
 * @description:
 */
@DisplayName("车辆缓存测试")
public class VehicleCacheTest {

    private static Logger logger = LoggerFactory.getLogger(VehicleCacheTest.class);

    private VehicleCacheTest() {
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
        final VehicleCache cache = VehicleCache.getInstance();
        cache.invalidateAll();
    }

    @Test
    void testGetVehicleCache1()
        throws ExecutionException {

        final String vid = "a03edac3-2720-4455-a755-529fe1b3e728";

        final VehicleCache cache = VehicleCache.getInstance();

        final HashSet<String> fields = new HashSet<>();
        fields.add("testField");
        fields.add("json");
        fields.add("empty");
        fields.add("nullptr");
        final Map<String, ImmutableMap<String, String>> values = cache.getFields(
            vid, fields);
        logger.trace("values=[{}]", values);
    }

    @Test
    void testGetVehicleCache2()
        throws ExecutionException {

        final String vid = "a03edac3-2720-4455-a755-529fe1b3e728";

        final VehicleCache cache = VehicleCache.getInstance();

        final Map<String, String> testValue = cache.getField(
            vid, "testField");
        logger.trace("testValue=[{}]", testValue);
        //Assertions.assertEquals("testValue", testValue);

        final Map<String, String> json = cache.getField(
            vid, "json");
        logger.trace("json=[{}]", json);

        final Map<String, String> empty = cache.getField(
            vid, "empty");
        logger.trace("empty=[{}]", empty);

        final Map<String, String> nullptr = cache.getField(
            vid, "nullptr");
        logger.trace("nullptr=[{}]", nullptr);
    }

    @Test
    void testGetVehicleCache3()
        throws ExecutionException {

        final String vid = "a03edac3-2720-4455-a755-529fe1b3e728";

        final VehicleCache cache = VehicleCache.getInstance();

        final Map<String, ImmutableMap<String, String>> values = cache.getFields(
            vid,
            "testField", "json", "empty", "nullptr");
        logger.trace("values=[{}]", values);
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
        final VehicleCache cache = VehicleCache.getInstance();
        cache.invalidateAll();
    }
}
