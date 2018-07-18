package storm;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-07-18
 * @description:
 */
@DisplayName("NumberUtilsTesrt测试")
final class NumberUtilsTesrt {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NumberUtilsTesrt.class);

    private NumberUtilsTesrt() {
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

    @DisplayName("测试数字校验")
    @Test
    void testMethod() {

        final String partHex = "FEDCBA91";
        final String fullHex = "0xFEDCBA91";


        Assertions.assertFalse(NumberUtils.isDigits(partHex));
        Assertions.assertFalse(NumberUtils.isDigits(fullHex));
        Assertions.assertTrue(NumberUtils.isNumber(fullHex));
        Assertions.assertFalse(NumberUtils.isNumber(partHex));
        Assertions.assertFalse(StringUtils.startsWithAny(partHex, new String[] {"0x", "0X"}));
        Assertions.assertTrue(StringUtils.startsWithAny(fullHex, new String[] {"0x", "0X"}));
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
