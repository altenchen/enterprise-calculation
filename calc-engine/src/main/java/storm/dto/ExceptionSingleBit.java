package storm.dto;

import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-06-24
 * @description: 按位解析-故障码异常, 仅限单个位
 */
public class ExceptionSingleBit {

    /**
     * 异常码Id
     */
    @NotNull
    public final String exceptionId;

    /**
     * 码值偏移
     */
    public final short offset;

    /**
     * 告警延时
     */
    public final int lazy;

    /**
     * 告警级别
     */
    public final byte level;

    public ExceptionSingleBit(@NotNull String exceptionId, short offset, int lazy, byte level) {
        this.exceptionId = exceptionId;
        this.offset = offset;
        this.lazy = lazy;
        this.level = level;
    }
}
