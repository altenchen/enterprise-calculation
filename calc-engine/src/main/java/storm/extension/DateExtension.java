package storm.extension;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;

/**
 * 时间日期扩展类
 * @author: xzp
 * @date: 2018-12-18
 * @description:
 */
public final class DateExtension {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DateExtension.class);

    /**
     * 获取日期部分
     * @param time 时间
     * @return 日期部分
     */
    @Contract("_ -> new")
    @NotNull
    public static Date getDate(@NotNull final Date time) {
        return new Date(getDate(time.getTime()));
    }

    /**
     * 获取日期部分
     * @param millisecond 时间
     * @return 日期部分
     */
    public static long getDate(final long millisecond) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millisecond);
        calendar.setLenient(false);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }

    /**
     * 获取日期部分
     * @param time 时间
     * @return 日期部分
     */
    @NotNull
    public static long getMillisecondOfDay(@NotNull final Date time) {
        return getMillisecondOfDay(time.getTime());
    }

    /**
     * 获取日期部分
     * @param millisecond 时间
     * @return 日期部分
     */
    public static long getMillisecondOfDay(final long millisecond) {
        return millisecond - getDate(millisecond);
    }

    /**
     * 获取时间是周几
     * @param time 时间
     * @return 时间是周几
     * @see Calendar::DAY_OF_WEEK
     */
    public static int getDayOfWeek(@NotNull final Date time) {
        return getDayOfWeek(time.getTime());
    }

    /**
     * 获取时间是周几
     * @param millisecond 时间
     * @return 时间是周几
     * @see Calendar::DAY_OF_WEEK
     */
    public static int getDayOfWeek(final long millisecond) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millisecond);
        return calendar.get(Calendar.DAY_OF_WEEK);
    }
}
