package storm.tool;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

/**
 * @author: xzp
 * @date: 2018-09-30
 * @description: 延迟开关
 */
public final class DelaySwitch {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DelaySwitch.class);

    // region 正向区域

    /**
     * 正向连续计数阈值
     */
    private final int positiveThreshold;

    /**
     * 正向连续超时阈值
     */
    private final long positiveTimeout;

    // endregion 正向区域

    // region 反向区域

    private static final int NEGATIVE_INIT_COUNT = -1;

    /**
     * 反向连续计数阈值
     */
    private final int negativeThreshold;

    /**
     * 反向连续超时阈值
     */
    private final long negativeTimeout;

    // endregion 反向区域

    /**
     * @param positiveThreshold 正向连续计数阈值
     * @param positiveTimeout 正向连续超时阈值
     * @param negativeThreshold 反向连续计数阈值
     * @param negativeTimeout 反向连续超时阈值
     */
    public DelaySwitch(
        final int positiveThreshold,
        final long positiveTimeout,
        final int negativeThreshold,
        final long negativeTimeout
    ) {
        if(positiveThreshold < 0) {
            throw new IllegalArgumentException("正向连续计数阈值必须不小于0");
        }
        this.positiveThreshold = positiveThreshold;
        if(positiveTimeout < 0) {
            throw new IllegalArgumentException("正向连续超时阈值必须不小于0");
        }
        this.positiveTimeout = positiveTimeout;

        if(negativeThreshold < 0) {
            throw new IllegalArgumentException("反向连续计数阈值必须不小于0");
        }
        this.negativeThreshold = -negativeThreshold;
        if(negativeTimeout < 0) {
            throw new IllegalArgumentException("反向连续超时阈值必须不小于0");
        }
        this.negativeTimeout = negativeTimeout;
    }

    /**
     * 获取开关状态, 负数-反向, 0-未知, 正数-正向
     */
    @Contract(pure = true)
    public int getSwitchStatus() {
        return switchStatus;
    }

    /**
     * 设置开关状态, 负数-反向, 0-未知, 正数-正向
     */
    public void setSwitchStatus(int switchStatus) {
        this.switchStatus = switchStatus;
    }

    /**
     * 连续计数器
     */
    private int continueCount = 0;

    /**
     * 重置时间
     */
    private long initTimeMillisecond = 0;

    /**
     * 开关状态, -1-反向, 0-未知, 1-正向
     */
    private int switchStatus;

    /**
     * 正向增长
     * @param timeMillisecond 数据时间
     * @param positiveReset 正向重置行为
     * @param positiveOverflow 正向溢出行为
     */
    public void positiveIncrease(
        final long timeMillisecond,
        @NotNull final Runnable positiveReset,
        @NotNull final BiConsumer<Integer, Long> positiveOverflow) {
        if (switchStatus <= 0) {
            if (continueCount < 1) {
                continueCount = 1;
                initTimeMillisecond = timeMillisecond;
                positiveReset.run();
            }

            if (continueCount < positiveThreshold) {
                continueCount += 1;
            } else if (timeMillisecond - initTimeMillisecond >= positiveTimeout) {
                positiveOverflow.accept(positiveThreshold, positiveTimeout);
                switchStatus = 1;
            }
        } else {
            continueCount = 0;
        }
    }

    /**
     * 反向增长
     * @param timeMillisecond 数据时间
     * @param negativeReset 反向重置行为
     * @param negativeOverflow 反向溢出行为
     */
    public void negativeIncrease(
        final long timeMillisecond,
        @NotNull final Runnable negativeReset,
        @NotNull final BiConsumer<Integer, Long> negativeOverflow) {
        if (switchStatus >= 0) {
            if (continueCount > -1) {
                continueCount = -1;
                initTimeMillisecond = timeMillisecond;
                negativeReset.run();
            }

            if (continueCount > negativeThreshold) {
                continueCount -= 1;
            } else if (timeMillisecond - initTimeMillisecond >= negativeTimeout) {
                negativeOverflow.accept(negativeThreshold, negativeTimeout);
                switchStatus = -1;
            }
        } else {
            continueCount = 0;
        }
    }
}
