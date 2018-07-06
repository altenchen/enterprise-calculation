package storm;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;

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
    }

    @Test
    public void testGetVehicleCache()
        throws ExecutionException {

        final VehicleCache cache = VehicleCache.getInstance();

        final String testValue = cache.getVehicleCache("a03edac3-2720-4455-a755-529fe1b3e728", "testField");
        logger.trace("testValue=[{}]", testValue);
        //Assertions.assertEquals("testValue", testValue);

        final String json = cache.getVehicleCache("a03edac3-2720-4455-a755-529fe1b3e728", "json");
        logger.trace("json=[{}]", json);

        final String empty = cache.getVehicleCache("a03edac3-2720-4455-a755-529fe1b3e728", "empty");
        logger.trace("empty=[{}]", empty);

        final String nullref = cache.getVehicleCache("a03edac3-2720-4455-a755-529fe1b3e728", "nullref");
        if (nullref == null) {
            logger.trace("nullref=[空引用]");
        } else {
            logger.trace("nullref=[{}]", nullref);
        }

        cache.getVehicleCache("a03edac3-2720-4455-a755-529fe1b3e728", "reload");
        cache.getVehicleCache("a03edac3-2720-4455-a755-529fe1b3e728", "reload");
        cache.getVehicleCache("a03edac3-2720-4455-a755-529fe1b3e728", "reload");
        cache.getVehicleCache("a03edac3-2720-4455-a755-529fe1b3e728", "reload");
        cache.getVehicleCache("a03edac3-2720-4455-a755-529fe1b3e728", "reload");
    }

    @Test
    public void testGetVehicleCaches()
        throws ExecutionException {

//        final VehicleCache cache = VehicleCache.getInstance();
//
//        final Map<String, String> values = cache.getVehicleCaches("a03edac3-2720-4455-a755-529fe1b3e728");
//        values.forEach((k, v) -> {
//            logger.trace("{}=[{}]", k, v);
//        });
    }

    @SuppressWarnings("unused")
    @AfterEach
    public void afterEach() {
        // 每个测试之后
    }

    @SuppressWarnings("unused")
    @AfterAll
    public static void afterAll() {
        // 所有测试之后
    }
}
