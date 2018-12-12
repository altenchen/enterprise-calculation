package storm.tool;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

/**
 * @author: xzp
 * @date: 2018-09-30
 * @description: 双路延迟开关
 */
public final class DelaySwitch {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DelaySwitch.class);

    // region 正向区域

    /**
     * 正向连续计数阈值
     */
    private int positiveThreshold;

    /**
     * 正向连续超时阈值
     */
    private long positiveTimeout;

    // endregion 正向区域

    // region 反向区域

    /**
     * 反向连续计数阈值
     */
    private int negativeThreshold;

    /**
     * 反向连续超时阈值
     */
    private long negativeTimeout;

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
     * 获取开关状态, false-反向, null-未知, true-正向
     */
    @Nullable
    @Contract(pure = true)
    public Boolean getSwitchStatus() {
        final int switchStatus = this.switchStatus;
        if(switchStatus > 0) {
            return true;
        } else if(switchStatus < 0) {
            return false;
        } else {
            return null;
        }
    }

    /**
     * 设置开关状态, false-反向, null-未知, true-正向
     */
    @Contract("_ -> this")
    public DelaySwitch setSwitchStatus(@Nullable final Boolean switchStatus) {
        if(null == switchStatus) {
            this.switchStatus = 0;
        } else if(switchStatus) {
            this.switchStatus = 1;
        } else {
            this.switchStatus = -1;
        }
        return this;
    }

    /**
     * 连续计数
     */
    private int continueCount = 0;

    /**
     * 重置时间
     */
    private long resetTimeMillisecond = 0;

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
                resetTimeMillisecond = timeMillisecond;
                positiveReset.run();
            }

            if (continueCount < positiveThreshold) {
                continueCount += 1;
            } else if (timeMillisecond - resetTimeMillisecond >= positiveTimeout) {
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
                resetTimeMillisecond = timeMillisecond;
                negativeReset.run();
            }

            if (continueCount > negativeThreshold) {
                continueCount -= 1;
            } else if (timeMillisecond - resetTimeMillisecond >= negativeTimeout) {
                negativeOverflow.accept(-negativeThreshold, negativeTimeout);
                switchStatus = -1;
            }
        } else {
            continueCount = 0;
        }
    }

    /**
     * 正向连续计数阈值
     */
    @Contract(pure = true)
    public int getPositiveThreshold() {
        return positiveThreshold;
    }

    /**
     * 正向连续计数阈值
     */
    @Contract("_ -> this")
    public DelaySwitch setPositiveThreshold(final int positiveThreshold) {
        this.positiveThreshold = positiveThreshold;
        return this;
    }

    /**
     * 正向连续超时阈值
     */
    @Contract(pure = true)
    public long getPositiveTimeout() {
        return positiveTimeout;
    }

    /**
     * 正向连续超时阈值
     */
    @Contract("_ -> this")
    public DelaySwitch setPositiveTimeout(final long positiveTimeout) {
        this.positiveTimeout = positiveTimeout;
        return this;
    }

    /**
     * 反向连续计数阈值
     */
    @Contract(pure = true)
    public int getNegativeThreshold() {
        return negativeThreshold;
    }

    /**
     * 反向连续计数阈值
     */
    @Contract("_ -> this")
    public DelaySwitch setNegativeThreshold(final int negativeThreshold) {
        this.negativeThreshold = negativeThreshold;
        return this;
    }

    /**
     * 反向连续超时阈值
     */
    @Contract(pure = true)
    public long getNegativeTimeout() {
        return negativeTimeout;
    }

    /**
     * 反向连续超时阈值
     */
    @Contract("_ -> this")
    public DelaySwitch setNegativeTimeout(final long negativeTimeout) {
        this.negativeTimeout = negativeTimeout;
        return this;
    }
}
