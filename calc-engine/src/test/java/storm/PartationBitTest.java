package storm;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.PartationBit;

/**
 * @author: xzp
 * @date: 2018-07-19
 * @description:
 */
@DisplayName("位区测试")
final class PartationBitTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(PartationBitTest.class);

    private PartationBitTest() {
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

    @DisplayName("低字节最高位")
    @Test
    void 低字节最高位() {
        final long code_30_2_0 = PartationBit.computeValue(new long[]{0x3FFFFFFFL, 0xFFFFFFFFL}, (short)30, (byte)2);
        Assertions.assertEquals(0, code_30_2_0);
        final long code_30_2_1 = PartationBit.computeValue(new long[]{0x5FFFFFFFL, 0xFFFFFFFFL}, (short)30, (byte)2);
        Assertions.assertEquals(1, code_30_2_1);
        final long code_30_2_2 = PartationBit.computeValue(new long[]{0xBFFFFFFFL, 0xFFFFFFFEL}, (short)30, (byte)2);
        Assertions.assertEquals(2, code_30_2_2);
        final long code_30_2_3 = PartationBit.computeValue(new long[]{0xDFFFFFFFL, 0xFFFFFFFEL}, (short)30, (byte)2);
        Assertions.assertEquals(3, code_30_2_3);
    }

    @DisplayName("高字节最低位")
    @Test
    void 高字节最低位() {
        final long code_32_2_0 = PartationBit.computeValue(new long[]{0xFFFFFFFFL, 0xFFFFFFFCL}, (short)32, (byte)2);
        Assertions.assertEquals(0, code_32_2_0);
        final long code_32_2_1 = PartationBit.computeValue(new long[]{0x7FFFFFFFL, 0xFFFFFFFDL}, (short)32, (byte)2);
        Assertions.assertEquals(1, code_32_2_1);
        final long code_32_2_2 = PartationBit.computeValue(new long[]{0xFFFFFFFFL, 0xFFFFFFFAL}, (short)32, (byte)2);
        Assertions.assertEquals(2, code_32_2_2);
        final long code_32_2_3 = PartationBit.computeValue(new long[]{0x7FFFFFFFL, 0xFFFFFFFBL}, (short)32, (byte)2);
        Assertions.assertEquals(3, code_32_2_3);
    }

    @DisplayName("双字节衔接位")
    @Test
    void 双字节衔接位() {
        final long code_31_2_0 = PartationBit.computeValue(new long[]{0x7FFFFFFFL, 0xFFFFFFFEL}, (short)31, (byte)2);
        Assertions.assertEquals(0, code_31_2_0);
        final long code_31_2_1 = PartationBit.computeValue(new long[]{0xBFFFFFFFL, 0xFFFFFFFEL}, (short)31, (byte)2);
        Assertions.assertEquals(1, code_31_2_1);
        final long code_31_2_2 = PartationBit.computeValue(new long[]{0x7FFFFFFFL, 0xFFFFFFFDL}, (short)31, (byte)2);
        Assertions.assertEquals(2, code_31_2_2);
        final long code_31_2_3 = PartationBit.computeValue(new long[]{0xBFFFFFFFL, 0xFFFFFFFDL}, (short)31, (byte)2);
        Assertions.assertEquals(3, code_31_2_3);
    }

    @DisplayName("故障码测试")
    @Test
    void 故障码测试() {
        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x0L}, (short)0, (byte)1));
        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x0L}, (short)1, (byte)1));
        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x0L}, (short)2, (byte)1));

        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x1L}, (short)0, (byte)1));
        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x1L}, (short)1, (byte)1));
        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x1L}, (short)2, (byte)1));

        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x2L}, (short)0, (byte)1));
        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x2L}, (short)1, (byte)1));
        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x2L}, (short)2, (byte)1));

        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x3L}, (short)0, (byte)1));
        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x3L}, (short)1, (byte)1));
        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x3L}, (short)2, (byte)1));

        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x4L}, (short)0, (byte)1));
        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x4L}, (short)1, (byte)1));
        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x4L}, (short)2, (byte)1));

        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x5L}, (short)0, (byte)1));
        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x5L}, (short)1, (byte)1));
        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x5L}, (short)2, (byte)1));

        Assertions.assertEquals(0, PartationBit.computeValue(new long[]{0x6L}, (short)0, (byte)1));
        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x6L}, (short)1, (byte)1));
        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x6L}, (short)2, (byte)1));

        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x7L}, (short)0, (byte)1));
        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x7L}, (short)1, (byte)1));
        Assertions.assertEquals(1, PartationBit.computeValue(new long[]{0x7L}, (short)2, (byte)1));

        final long code = PartationBit.computeValue(new long[]{0x0L}, (short)0);
        Assertions.assertFalse(code != 0);
        Assertions.assertTrue(code == 0);
        Assertions.assertFalse(code != 0L);
        Assertions.assertTrue(code == 0L);
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
