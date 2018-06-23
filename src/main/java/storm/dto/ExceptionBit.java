package storm.dto;

import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-06-24
 * @description: 按位解析-故障码异常
 * @note
 * [2018年6月24日] 已完成, 暂不使用, 简化为按1bit表示处理, 有时间再整.
 */
public class ExceptionBit {

    /**
     * 异常码Id
     */
    @NotNull
    public final String exceptionId;

    /**
     * 异常码值
     */
    public final long value;

    /**
     * 告警延时
     */
    public final int lazy;

    /**
     * 告警级别
     */
    public final byte level;

    public ExceptionBit(
        @NotNull String exceptionId, long value,
        int lazy,
        byte level) {

        this.exceptionId = exceptionId;
        this.value = value;
        this.lazy = lazy;
        this.level = level;
    }
}
