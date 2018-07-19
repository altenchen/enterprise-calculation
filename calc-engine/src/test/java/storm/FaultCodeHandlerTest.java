package storm;

import org.apache.commons.lang.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.handler.FaultCodeHandler;

/**
 * @author: xzp
 * @date: 2018-07-19
 * @description:
 */
@DisplayName("FaultCodeHandlerTest测试")
final class FaultCodeHandlerTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(FaultCodeHandlerTest.class);

    private FaultCodeHandlerTest() {
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
        final String codeValues = "0";
        final @NotNull long[] values = FaultCodeHandler.parseFaultCodes("0");
        logger.trace("故障码解析[{}]->[{}].", ArrayUtils.toString(codeValues), ArrayUtils.toString(values));
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
