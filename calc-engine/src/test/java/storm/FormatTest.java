package storm;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

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

    @DisplayName("测试方法")
    @Test
    void testFormat() throws ParseException {

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