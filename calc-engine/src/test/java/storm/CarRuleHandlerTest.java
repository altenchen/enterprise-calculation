package storm;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-07-19
 * @description:
 */
@DisplayName("CarRuleHandlerTest测试")
final class CarRuleHandlerTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CarRuleHandlerTest.class);

    private CarRuleHandlerTest() {
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
