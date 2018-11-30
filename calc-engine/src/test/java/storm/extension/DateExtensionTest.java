package storm.extension;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-12-18
 * @description:
 */
@DisplayName("时间日期扩展类测试")
final class DateExtensionTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DateExtensionTest.class);

    private DateExtensionTest() {
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

    @DisplayName("测试获取日期部分")
    @Test
    void testGetDate() {
        final Calendar calendar = Calendar.getInstance();

        calendar.set(2018,Calendar.DECEMBER,31);
        final Date _2018_12_31 = DateExtension.getDate(calendar.getTime());

        calendar.set(2018,Calendar.DECEMBER,31,0,0,0);
        Assertions.assertEquals(_2018_12_31, DateExtension.getDate(calendar.getTime()));
        Assertions.assertEquals(_2018_12_31.getTime(), DateExtension.getDate(calendar.getTimeInMillis()));

        calendar.set(2018,Calendar.DECEMBER,31,23,59,59);
        Assertions.assertEquals(_2018_12_31, DateExtension.getDate(calendar.getTime()));
        Assertions.assertEquals(_2018_12_31.getTime(), DateExtension.getDate(calendar.getTimeInMillis()));

    }

    @DisplayName("测试获取时间部分")
    @Test
    void testGetMillisecondOfDay() {

        final Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MILLISECOND, 0);

        final long _22_33_44 = TimeUnit.HOURS.toMillis(22)
            + TimeUnit.MINUTES.toMillis(33)
            + TimeUnit.SECONDS.toMillis(44);

        calendar.set(2018,Calendar.DECEMBER,31, 22, 33, 44);
        Assertions.assertEquals(_22_33_44, DateExtension.getMillisecondOfDay(calendar.getTime()));
        Assertions.assertEquals(_22_33_44, DateExtension.getMillisecondOfDay(calendar.getTimeInMillis()));

        calendar.set(2019,Calendar.JANUARY,1, 22, 33, 44);
        Assertions.assertEquals(_22_33_44, DateExtension.getMillisecondOfDay(calendar.getTime()));
        Assertions.assertEquals(_22_33_44, DateExtension.getMillisecondOfDay(calendar.getTimeInMillis()));

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
