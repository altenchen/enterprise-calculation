package storm.kafka.spout;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-08-11
 * @description:
 */
@DisplayName("GeneralRecordTranslatorTest")
public class GeneralRecordTranslatorTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(GeneralRecordTranslatorTest.class);

    private GeneralRecordTranslatorTest() {
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
        // 测试代码
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
