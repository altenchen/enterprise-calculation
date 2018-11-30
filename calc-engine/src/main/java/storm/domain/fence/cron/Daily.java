package storm.domain.fence.cron;

import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 日常激活计划
 * @author: xzp
 * @date: 2018-11-30
 * @description:
 */
public final class Daily implements Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(Daily.class);

    private static final long MILLISECOND_OF_DAY = TimeUnit.DAYS.toMillis(1);

    /**
     * 开始时间
     */
    private final long startTime;

    /**
     * 结束时间
     */
    private final long stopTime;

    public Daily(long startTime, long stopTime) {

        this.startTime = startTime % MILLISECOND_OF_DAY;
        this.stopTime = stopTime % MILLISECOND_OF_DAY;
    }

    @Contract(pure = true)
    @Override
    public boolean active(final long dateTime) {

        if(startTime == stopTime) {
            return true;
        }

        final long point = dateTime % MILLISECOND_OF_DAY;

        if(startTime < stopTime) {
            return point >= startTime && point < stopTime;
        } else{
            return point < stopTime || point >= startTime;
        }
    }
}
