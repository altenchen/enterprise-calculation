package storm.domain.fence.cron;

import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.DateExtension;

import java.util.concurrent.TimeUnit;

/**
 * 每周激活计划
 * @author: xzp
 * @date: 2018-11-30
 * @description:
 */
public final class WeeklyCycle implements Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(WeeklyCycle.class);

    private static final long DAILY_MILLISECOND = TimeUnit.DAYS.toMillis(1);

    private static final byte DAYS_OF_WEEKLY = 7;

    /**
     * 空三二一日六五四
     */
    private final byte weekly_flag;

    private final Daily daily;

    /**
     *
     * @param weekly_flag 每周标记, 空六五四三二一日
     * @param start 每天开始时间
     * @param stop 每天停止时间
     */
    public WeeklyCycle(final byte weekly_flag, long start, long stop) {
        this.weekly_flag = (byte)(((weekly_flag & 0x0F) << 3) | ((weekly_flag & 0x70) >>> 4));
        this.daily = new Daily(start, stop);
    }

    @Contract(pure = true)
    @Override
    public boolean active(final long dateTime) {

        if ((weekly_flag >> ((dateTime / DAILY_MILLISECOND) % DAYS_OF_WEEKLY) & 1) != 1) {
            return false;
        }

        return daily.active(DateExtension.getMillisecondOfDay(dateTime));
    }
}
