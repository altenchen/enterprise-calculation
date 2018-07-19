package storm;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author: xzp
 * @date: 2018-07-19
 * @description:
 */
@DisplayName("CarOnOffHandlerTest测试")
final class CarOnOffHandlerTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CarOnOffHandlerTest.class);

    private CarOnOffHandlerTest() {
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
        final Map<String, Object> notice = new TreeMap<>();
        final String key = "smileage";

        final Object smileage1 = notice.get(key);
        if(smileage1 == null) {

            notice.put(key, -1);

            final Object smileage2 = notice.get(key);
            if("-1".equals(smileage2.toString())) {
                notice.put(key, 8);

                final Object smileage3 = notice.get(key);
                if(smileage3 == null || "-1".equals(smileage3.toString())) {
                    Assertions.fail();
                }


            } else {
                Assertions.fail();
            }

        } else {
            Assertions.fail();
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
