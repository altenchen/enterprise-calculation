package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.util.JsonUtils;

/**
 * @author 徐志鹏
 * 偏移系数定义
 */
public final class CoefficientOffset {

    @NotNull
    private final String dataKey;

    private final double coefficient;

    private final double offset;


    public CoefficientOffset(
        @NotNull final String dataKey,
        double coefficient,
        double offset) {

        if(StringUtils.isBlank(dataKey)) {
            throw new IllegalArgumentException("数据键不能为空白");
        }
        if(-Double.MIN_NORMAL < coefficient && coefficient < Double.MIN_NORMAL) {
            throw new IllegalArgumentException("系数不能为0");
        }
        this.dataKey = dataKey;
        this.coefficient = coefficient;
        this.offset = offset;
    }

    @Nullable
    public Double compute(
        @NotNull final ImmutableMap<String, String> data) {

        final String string = data.get(getDataKey());
        if(!NumberUtils.isNumber(string)) {
            return null;
        }

        final double value = NumberUtils.toDouble(string);
        return (value - getOffset()) / getCoefficient();
    }

    @Contract(pure = true)
    public double compute(double value) {
        return (value - getOffset()) / getCoefficient();
    }

    /**
     * 数据键
     */
    @Contract(pure = true)
    public String getDataKey() {
        return dataKey;
    }

    /**
     * 系数
     */
    @Contract(pure = true)
    public double getCoefficient() {
        return coefficient;
    }

    /**
     * 偏移值
     */
    @Contract(pure = true)
    public double getOffset() {
        return offset;
    }

    @NotNull
    @Override
    public String toString() {
        return JsonUtils.getInstance().toJson(this);
    }
}
                                                  