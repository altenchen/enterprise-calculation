package storm.domain.fence.cron;

import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: xzp
 * @date: 2018-11-30
 * @description:
 */
public final class Once implements Cron {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(Once.class);

    /**
     * 开始时间
     */
    private final long start;

    /**
     * 结束时间
     */
    private final long stop;

    public Once(long start, long stop) {
        this.start = start;
        this.stop = stop;
    }

    @Contract(pure = true)
    @Override
    public boolean active(final long time) {
        return time >= start && time <= stop;
    }
}
