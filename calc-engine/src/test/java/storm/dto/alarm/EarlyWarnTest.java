package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author: xzp
 * @date: 2018-09-27
 * @description:
 */
@DisplayName("EarlyWarnTest")
public final class EarlyWarnTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(EarlyWarnTest.class);

    private EarlyWarnTest() {
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

    @DisplayName("平台报警_数据摘取函数构建")
    @Test
    void testMapGetter() {

        final Function<ImmutableMap<String, String>, BigDecimal> function = EarlyWarn.buildDataGetter("2202");
        Assertions.assertTrue(new BigDecimal("1234.5").compareTo(function.apply(ImmutableMap.of("2202", "12345"))) == 0);
        Assertions.assertNull(function.apply(ImmutableMap.of("7615", "80")));
        Assertions.assertNull(function.apply(ImmutableMap.of("2202", "")));
        Assertions.assertNull(function.apply(ImmutableMap.of("2202", " ")));
    }

    @DisplayName("平台报警_数据摘取函数构建_实时")
    @Test
    void testDataGetter() {

        final BiFunction<ImmutableMap<String, String>, ImmutableMap<String, String>, BigDecimal> function = EarlyWarn.buildDataGetter(false, "2202");
        Assertions.assertTrue(new BigDecimal("1234.5").compareTo(
            function.apply(
                ImmutableMap.of("2202", "12345"),

                ImmutableMap.of("2202", "98765"))) == 0);
    }

    @DisplayName("平台报警_数据摘取函数构建_缓存")
    @Test
    void testCacheGetter() {

        final BiFunction<ImmutableMap<String, String>, ImmutableMap<String, String>, BigDecimal> function = EarlyWarn.buildDataGetter(true, "2202");
        Assertions.assertTrue(new BigDecimal("9876.5").compareTo(
            function.apply(
                ImmutableMap.of("2202", "12345"),

                ImmutableMap.of("2202", "98765"))) == 0);
    }

    @DisplayName("平台报警_算术运算函数构建 -> L1 + L2")
    @Test
    void testPlusArithmeticExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L1 + L2";
        final BigDecimal left1Value = new BigDecimal("98.7");
        final BigDecimal left2Value = new BigDecimal("1.23");
        final BigDecimal result = new BigDecimal("99.93");

        final BiFunction<BigDecimal, BigDecimal, BigDecimal> function = EarlyWarn.buildArithmeticFunction(
            ruleId,
            ruleName,
            "1"
        );
        Assertions.assertNotNull(function);
        Assertions.assertNull(function.apply(left1Value, null));
        Assertions.assertNull(function.apply(null, left2Value));
        Assertions.assertEquals(result, function.apply(left1Value, left2Value));
    }

    @DisplayName("平台报警_算术运算函数构建 -> L1 - L2")
    @Test
    void testSubtractArithmeticExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L1 + L2";
        final BigDecimal left1Value = new BigDecimal("98.7");
        final BigDecimal left2Value = new BigDecimal("1.23");
        final BigDecimal result = new BigDecimal("97.47");

        final BiFunction<BigDecimal, BigDecimal, BigDecimal> function = EarlyWarn.buildArithmeticFunction(
            ruleId,
            ruleName,
            "2"
        );
        Assertions.assertNotNull(function);
        Assertions.assertNull(function.apply(left1Value, null));
        Assertions.assertNull(function.apply(null, left2Value));
        Assertions.assertEquals(result, function.apply(left1Value, left2Value));
    }

    @DisplayName("平台报警_算术运算函数构建 -> L1 * L2")
    @Test
    void testMultiplyArithmeticExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L1 + L2";
        final BigDecimal left1Value = new BigDecimal("1.020304");
        final BigDecimal left2Value = new BigDecimal("1.1");
        final BigDecimal result = new BigDecimal("1.1223344");

        final BiFunction<BigDecimal, BigDecimal, BigDecimal> function = EarlyWarn.buildArithmeticFunction(
            ruleId,
            ruleName,
            "3"
        );
        Assertions.assertNotNull(function);
        Assertions.assertNull(function.apply(left1Value, null));
        Assertions.assertNull(function.apply(null, left2Value));
        Assertions.assertEquals(result, function.apply(left1Value, left2Value));
    }

    @DisplayName("平台报警_算术运算函数构建 -> L1 / L2")
    @Test
    void testDivideArithmeticExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L1 + L2";
        final BigDecimal left1Value = new BigDecimal("11.223344");
        final BigDecimal left2Value = new BigDecimal("1.1");
        final BigDecimal result = new BigDecimal("10.203040");

        final BiFunction<BigDecimal, BigDecimal, BigDecimal> function = EarlyWarn.buildArithmeticFunction(
            ruleId,
            ruleName,
            "4"
        );
        Assertions.assertNotNull(function);
        Assertions.assertNull(function.apply(left1Value, null));
        Assertions.assertNull(function.apply(null, left2Value));
        Assertions.assertEquals(result, function.apply(left1Value, left2Value));
        Assertions.assertEquals(new BigDecimal("0.7"), function.apply(new BigDecimal("2.0"), new BigDecimal("3.0")));
        Assertions.assertEquals(new BigDecimal("0.67"), function.apply(new BigDecimal("2.00"), new BigDecimal("3.0")));
        Assertions.assertEquals(new BigDecimal("0.67"), function.apply(new BigDecimal("2.0"), new BigDecimal("3.00")));
        Assertions.assertEquals(new BigDecimal("0.429"), function.apply(new BigDecimal("3.000"), new BigDecimal("7.000")));
        Assertions.assertEquals(new BigDecimal("-0.429"), function.apply(new BigDecimal("-3.000"), new BigDecimal("7.000")));
    }

    @DisplayName("平台报警_逻辑运算函数构建_右一值无效")
    @Test
    void testInvalidRight1ValueLogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "平台报警_逻辑运算函数构建_右一值无效";
        final String right2Value = "98.7";

        Assertions.assertNull(EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "1",
            "",
            right2Value
        ));
        Assertions.assertNull(EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "1",
            "string",
            right2Value
        ));
    }

    @DisplayName("平台报警_逻辑运算函数构建 -> L = R1")
    @Test
    void testEqualLogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L = R1";
        final String right1Value = "1.23";

        final Function<Function<Integer, BigDecimal>, Boolean> function = EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "1",
            right1Value,
            null
        );
        Assertions.assertNotNull(function);
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.23000")));
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("98.7000")));
        Assertions.assertNull(function.apply(scale -> null));
    }

    @DisplayName("平台报警_逻辑运算函数构建 -> L < R1")
    @Test
    void testLessThanLogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L < R1";
        final String right1Value = "1.23";

        final Function<Function<Integer, BigDecimal>, Boolean> function = EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "2",
            right1Value,
            null
        );
        Assertions.assertNotNull(function);
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.22")));
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("1.23")));
        Assertions.assertNull(function.apply(scale -> null));
    }

    @DisplayName("平台报警_逻辑运算函数构建 -> L <= R1")
    @Test
    void testLessOrEqualLogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L <= R1";
        final String right1Value = "1.23";

        final Function<Function<Integer, BigDecimal>, Boolean> function = EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "3",
            right1Value,
            null
        );
        Assertions.assertNotNull(function);
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.22")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.23")));
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("1.24")));
        Assertions.assertNull(function.apply(scale -> null));
    }

    @DisplayName("平台报警_逻辑运算函数构建 -> L > R1")
    @Test
    void testMoreThanLogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L > R1";
        final String right1Value = "1.23";

        final Function<Function<Integer, BigDecimal>, Boolean> function = EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "4",
            right1Value,
            null
        );
        Assertions.assertNotNull(function);
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.24")));
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("1.23")));
        Assertions.assertNull(function.apply(scale -> null));
    }

    @DisplayName("平台报警_逻辑运算函数构建 -> L >= R1")
    @Test
    void testMoreOrEqualLogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L >= R1";
        final String right1Value = "1.23";

        final Function<Function<Integer, BigDecimal>, Boolean> function = EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "5",
            right1Value,
            null
        );
        Assertions.assertNotNull(function);
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.24")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.23")));
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("1.22")));
        Assertions.assertNull(function.apply(scale -> null));
    }

    @DisplayName("平台报警_逻辑运算函数构建 -> L ∈ (R1, R2)")
    @Test
    void testMoreR1AndLessR2LogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L ∈ (R1, R2)";
        final String right1Value = "1.23";
        final String right2Value = "98.7";

        final Function<Function<Integer, BigDecimal>, Boolean> function = EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "6",
            right1Value,
            right2Value
        );
        Assertions.assertNotNull(function);
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("1.23")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.24")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("98.6")));
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("98.7")));
        Assertions.assertNull(function.apply(scale -> null));
    }

    @DisplayName("平台报警_逻辑运算函数构建 -> L ∈ [R1, R2)")
    @Test
    void testMoreOrEqualR1AndLessR2LogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L ∈ [R1, R2)";
        final String right1Value = "1.23";
        final String right2Value = "98.7";

        final Function<Function<Integer, BigDecimal>, Boolean> function = EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "7",
            right1Value,
            right2Value
        );
        Assertions.assertNotNull(function);
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("1.22")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.23")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.24")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("98.6")));
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("98.7")));
        Assertions.assertNull(function.apply(scale -> null));
    }

    @DisplayName("平台报警_逻辑运算函数构建 -> L ∈ (R1, R2]")
    @Test
    void testMoreR1AndLessOrEqualR2LogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L ∈ (R1, R2]";
        final String right1Value = "1.23";
        final String right2Value = "98.7";

        final Function<Function<Integer, BigDecimal>, Boolean> function = EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "8",
            right1Value,
            right2Value
        );
        Assertions.assertNotNull(function);
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("1.23")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.24")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("98.6")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("98.7")));
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("98.8")));
        Assertions.assertNull(function.apply(scale -> null));
    }

    @DisplayName("平台报警_逻辑运算函数构建 -> L ∈ [R1, R2]")
    @Test
    void testMoreOrEqualR1AndLessOrEqualR2LogicExpression() {

        final String ruleId = UUID.randomUUID().toString();
        final String ruleName = "L ∈ [R1, R2]";
        final String right1Value = "1.23";
        final String right2Value = "98.7";

        final Function<Function<Integer, BigDecimal>, Boolean> function = EarlyWarn.buildLogicFunction(
            ruleId,
            ruleName,
            "9",
            right1Value,
            right2Value
        );
        Assertions.assertNotNull(function);
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("1.22")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.23")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("1.24")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("98.6")));
        Assertions.assertTrue(function.apply(scale -> new BigDecimal("98.7")));
        Assertions.assertFalse(function.apply(scale -> new BigDecimal("98.8")));
        Assertions.assertNull(function.apply(scale -> null));
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
