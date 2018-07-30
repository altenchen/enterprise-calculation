package storm;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;

import java.text.ParseException;
import java.util.*;

/**
 * @author: xzp
 * @date: 2018-07-08
 * @description:
 */
@DisplayName("格式化测试")
final class FormatTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(FormatTest.class);

    private FormatTest() {
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

    @Disabled("时间格式化和反格式化测试")
    @Test
    void timeFormat() throws ParseException {

        final TimeZone utcZone = TimeZone.getTimeZone("GMT");
        final TimeZone chinaZone = TimeZone.getTimeZone("Asia/Shanghai");
        final TimeZone chinaZoneToo = TimeZone.getTimeZone("GMT+8:00");

        final Calendar calendar = Calendar.getInstance();
        calendar.set(2018, 7-1, 8, 20, 52, 33);
        final Date time = calendar.getTime();
        final long timeInMillis = calendar.getTimeInMillis();
        final String format = "20180708205233";

        final String timeFormat = DateFormatUtils.format(time, FormatConstant.DATE_FORMAT);
        logger.trace("timeFormat->{}", timeFormat);
        Assertions.assertEquals(format, timeFormat);
        final String timeInMillisFormat = DateFormatUtils.format(timeInMillis, FormatConstant.DATE_FORMAT);
        logger.trace("timeInMillisFormat->{}", timeInMillisFormat);
        Assertions.assertEquals(format, timeInMillisFormat);

        final Date parseDate = DateUtils.parseDate(format, new String[]{FormatConstant.DATE_FORMAT});
        logger.trace("parseDate->{}", parseDate);
        Assertions.assertEquals(
            time.getTime() / DateUtils.MILLIS_PER_SECOND,
            parseDate.getTime() / DateUtils.MILLIS_PER_SECOND);

        final Map<String, Map<String, String>> hashMap = new HashMap<>();
        final Map<String, String> key1 = hashMap.getOrDefault("key", new HashMap<>());
        Assertions.assertNotNull(key1);
        final Map<String, String> key2 = hashMap.get("key");
        Assertions.assertNull(key2);
        hashMap.put("key", key1);
        final Map<String, String> key3 = hashMap.get("key");
        Assertions.assertNotNull(key3);
        final Map<String, String> key4 = hashMap.remove("key");
        Assertions.assertNotNull(key4);
        final Map<String, String> key5 = hashMap.remove("key");
        Assertions.assertNull(key5);
    }

    @Disabled("时间范围测试")
    @Test
    void timeRange() {
        try {
            final long tmp_last = 0;
            final long last = DateUtils.parseDate(String.valueOf(tmp_last), new String[]{FormatConstant.DATE_FORMAT}).getTime();
            logger.trace("last={}", last);
        } catch (ParseException e) {
            return;
        }
        Assertions.fail();
    }

    @Disabled("整数转二进制")
    @Test
    void integerToBinary() {
        final int integer = 0xF0F00F0F;
        final String longBinary = StringUtils.leftPad(Integer.toBinaryString(integer), Long.SIZE, '0');
        Assertions.assertEquals("0000000000000000000000000000000011110000111100000000111100001111", longBinary);
        final String shortBinary = StringUtils.leftPad(Integer.toBinaryString(integer), Short.SIZE, '0');
        Assertions.assertEquals("0000111100001111", shortBinary.substring(shortBinary.length() - Short.SIZE));
    }

    @Disabled("获取完整类名")
    @Test
    void getClassFullName() {
        final Object args = new TreeMap[4];
        final Class<?> type = args.getClass();
        final String name = type.getName();
        logger.debug("name = {}", name);
        final String canonicalName = type.getCanonicalName();
        logger.debug("canonicalName = {}", canonicalName);
        final String simpleName = type.getSimpleName();
        logger.debug("simpleName = {}", simpleName);
        final String typeName = type.getTypeName();
        logger.debug("typeName = {}", typeName);

        Assertions.assertEquals("org.apache.kafka.common.serialization.StringSerializer", org.apache.kafka.common.serialization.StringSerializer.class.getCanonicalName());
        Assertions.assertEquals("org.apache.kafka.common.serialization.StringSerializer", org.apache.kafka.common.serialization.StringSerializer.class.getCanonicalName());
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
