package storm.domain.fence.cron;

import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.DateExtension;

/**
 * 单次激活计划
 * @author: xzp
 * @date: 2018-11-30
 * @description:
 */
public final class DailyOnce implements Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DailyOnce.class);

    /**
     * 开始日期
     */
    private final long start_day;

    /**
     * 结束日期
     */
    private final long stop_day;

    private final Daily daily;

    public DailyOnce(
        long start_day,
        long stop_day,
        final long start_time,
        final long stop_time) {

        this.start_day = DateExtension.getDate(start_day);
        this.stop_day = DateExtension.getDate(stop_day);

        daily = new Daily(start_time, stop_time);
    }

    @Contract(pure = true)
    @Override
    public boolean active(final long dateTime) {

        if(start_day >= stop_day) {
            return false;
        }

        if(dateTime < start_day || dateTime >= stop_day) {
            return false;
        }

        return daily.active(DateExtension.getMillisecondOfDay(dateTime));
    }
}
