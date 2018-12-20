package storm.domain.fence.cron;

import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.DateExtension;

/**
 * 每天激活计划
 * @author: xzp
 * @date: 2018-11-30
 * @description:
 */
public final class DailyCycle implements Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DailyCycle.class);

    private final Daily daily;

    public DailyCycle(long start, long stop) {
        daily = new Daily(start, stop);
    }

    @Contract(pure = true)
    @Override
    public boolean active(final long dateTime) {
        return daily.active(DateExtension.getMillisecondOfDay(dateTime));
    }
}
