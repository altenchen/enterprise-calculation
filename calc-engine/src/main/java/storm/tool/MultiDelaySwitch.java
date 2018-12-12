package storm.tool;

import com.google.common.collect.Maps;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.function.TeConsumer;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author: xzp
 * @date: 2018-12-12
 * @description: 多路延迟开关
 * @param <S> 开关状态的类型
 */
public final class MultiDelaySwitch<S> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(MultiDelaySwitch.class);

    // region 连续计数阈值 Map<K, Integer> thresholdTimes

    /**
     * 连续计数阈值
     */
    private final Map<S, Integer> thresholdTimes = Maps.newHashMap();

    /**
     * 获取连续计数阈值
     * @param status 状态
     * @return 连续计数阈值, 不存在则返回空.
     */
    @Nullable
    public Integer getThresholdTimes(@Nullable final S status) {
        return Optional
            .ofNullable(status)
            .map(thresholdTimes::get)
            .orElse(null);
    }

    /**
     * 设置连续计数阈值
     * @param status 状态
     * @param thresholdTimes 连续计数阈值, 为 null 则删除配置, 否则取绝对值.
     * @return 当前对象自身
     */
    @NotNull
    @Contract("_, _ -> this")
    public MultiDelaySwitch<S> setThresholdTimes(@Nullable final S status, @Nullable final Integer thresholdTimes) {
        Optional
            .ofNullable(status)
            .ifPresent(s ->
                this.thresholdTimes.compute(
                    s,
                    (k, v) -> Optional
                        .ofNullable(thresholdTimes)
                        .map(Math::abs)
                        .orElse(null)
                )
            );
        return this;
    }

    // endregion 连续计数阈值 Map<K, Integer> thresholdTimes

    // region 连续超时阈值 Map<K, Long> timeoutMillisecond

    /**
     * 连续超时阈值
     */
    private final Map<S, Long> timeoutMillisecond = Maps.newHashMap();

    /**
     * 获取连续超时阈值
     * @param status 状态
     * @return 连续超时阈值
     */
    @Nullable
    public Long getTimeoutMillisecond(@Nullable final S status) {
        return Optional
            .ofNullable(status)
            .map(timeoutMillisecond::get)
            .orElse(null);
    }

    /**
     * 设置连续超时阈值
     * @param status 状态
     * @param timeoutMillisecond 连续超时阈值, 为 null 则删除配置, 否则取绝对值.
     * @return 当前对象自身
     */
    @NotNull
    @Contract("_, _ -> this")
    public MultiDelaySwitch<S> setTimeoutMillisecond(@Nullable final S status, @Nullable final Long timeoutMillisecond) {
        Optional
            .ofNullable(status)
            .ifPresent(s ->
                this.timeoutMillisecond.compute(
                    s,
                    (k, v) -> Optional
                        .ofNullable(timeoutMillisecond)
                        .map(Math::abs)
                        .orElse(null)
                )
            );
        return this;
    }

    // endregion 连续超时阈值 Map<K, Long> timeoutMillisecond

    /**
     * 连续状态
     */
    private S continueStatus;

    /**
     * 连续计数
     */
    private int continueCount = 0;

    /**
     * 重置时间
     */
    private long resetTimeMillisecond = 0;


    /**
     * 状态增长
     * @param status 状态
     * @param timeMillisecond 数据时间
     * @param resetCallback 重置回调
     * @param overflowCallback 溢出回调
     */
    public void increase(
        @Nullable final S status,
        final long timeMillisecond,
        @NotNull final Consumer<@Nullable S> resetCallback,
        @NotNull final TeConsumer<@Nullable S, Integer, Long> overflowCallback) {

        if(Objects.equals(status, this.switchStatus)) {
            continueStatus = status;
            return;
        }

        if (!Objects.equals(status, this.continueStatus)) {
            continueStatus = status;
            continueCount = 1;
            resetTimeMillisecond = timeMillisecond;
            resetCallback.accept(status);

        }

        final Integer thresholdTimes =
            this.thresholdTimes.getOrDefault(status, 1);
        if(continueCount < thresholdTimes) {
            continueCount += 1;
        } else {
            final Long timeoutMillisecond =
                this.timeoutMillisecond.getOrDefault(status, 0L);
            if (timeMillisecond - resetTimeMillisecond >= timeoutMillisecond) {
                overflowCallback.accept(status, thresholdTimes, timeoutMillisecond);
                this.switchStatus = status;
            }
        }
    }


    // region 开关状态 switchStatus

    /**
     * 获取开关状态
     */
    @Nullable
    @Contract(pure = true)
    public S getSwitchStatus() {
        return switchStatus;
    }

    /**
     * 设置开关状态
     */
    @NotNull
    @Contract("_ -> this")
    public MultiDelaySwitch<S> setSwitchStatus(@Nullable final S switchStatus) {
        this.switchStatus = switchStatus;
        return this;
    }

    /**
     * 开关状态
     */
    private S switchStatus;

    // endregion 开关状态 switchStatus
}
