package storm.domain.fence.cron;

/**
 * 激活计划
 * @author: xzp
 * @date: 2018-11-29
 * @description:
 */
public interface Cron {

    Cron DEFAULT = new Cron() {};

    /**
     * 判断给定的时间点是否处于激活的时间范围
     * @param dateTime 时间点
     * @return 给定的时间点是否处于激活的时间范围
     */
    default boolean active(long dateTime) {
        return true;
    }
}
