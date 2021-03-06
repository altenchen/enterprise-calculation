package storm.dto;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author: xzp
 * @date: 2018-06-22
 * @description: 按位解析-故障码分区
 * @note
 * [2018年6月24日] 未完成, 暂不使用, 简化为按1bit表示处理, 有时间再整.
 */
public final class PartationBit {

    @Contract(pure = true)
    public static boolean checkOffset(short offset) {
        return offset >= 0 && offset < (8 * 4 * 252);
    }

    @Contract(pure = true)
    public static boolean checkLength(byte length) {
        return length >= 1 && length <= 32;
    }

    @Contract(pure = true)
    public static boolean checkCode(long code) {
        return code >= 0 && code <= 0xFFFFFFFFL;
    }

    /**
     * 分区Id
     */
    @NotNull
    public final String partitionId;

    /**
     * 码值偏移 [0, 8 * 4 * 252)
     */
    public final short offset;

    /**
     * 码值位长 [1, 32]
     */
    public final byte length;

    /**
     * 恢复延时
     */
    public final int lazy;

    /**
     * 异常码值
     */
    private final Map<Long, ExceptionBit> exceptions;

    public PartationBit(
        @NotNull String partitionId,
        final short offset,
        final byte length,
        int lazy, final Map<Long, ExceptionBit> exceptions) {

        this.partitionId = partitionId;
        this.offset = offset;
        this.length = length;
        this.lazy = lazy;
        this.exceptions = exceptions;
    }

    /**
     * @param codes 码值
     * @return 匹配的异常码
     */
    @Contract(pure = true)
    @NotNull
    public Map<String, ExceptionBit> processFrame(@NotNull final long[] codes) {
        final TreeMap<String, ExceptionBit> result = new TreeMap<>();
        final long value = computeValue(codes);
        for (Long key : exceptions.keySet()) {
            final ExceptionBit exception = exceptions.get(key);
            if(value == exception.value) {
                result.put(exception.exceptionId, exception);
            }
        }
        return result;
    }

    /**
     * 提取故障码值, 如果相关位不存在, 会用0替代.
     * @param codes
     * @return 故障码值
     */
    @Nullable
    @Contract(pure = true)
    private long computeValue(@NotNull final long[] codes) {
        return computeValue(codes, this.offset, this.length);
    }

    @Contract(pure = true)
    public static long computeValue(@NotNull final long[] codes, final short offset) {
        return computeValue(codes, offset, (byte)1);
    }

    @Contract(pure = true)
    public static long computeValue(@NotNull final long[] codes, final short offset, final byte length) {

        final int lIndex = offset / 32;
        final long lValue = lIndex < codes.length ? codes[lIndex] : 0L;

        final int hIndex = (offset + length) / 32;
        final long hValue = hIndex < codes.length ? codes[hIndex] : 0L;

        final int lOffset = offset % 32;
        final int hOffset = (lOffset + length) % 32;

        if(lIndex == hIndex) {
            return ((lValue & (0xFFFFFFFFL >> (32 - hOffset))) >> lOffset);
        } else {
            return ((((hValue & (0xFFFFFFFFL >> (32 - hOffset))) << (32 - lOffset))) | ((lValue & 0xFFFFFFFFL) >> lOffset));
        }
    }
}
