package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.util.DataUtils;
import storm.util.JsonUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author 徐志鹏
 * 偏移系数定义
 */
public final class CoefficientOffset {

    @NotNull
    private final String itemId;

    @NotNull
    private final String dataKey;

    @NotNull
    private final BigDecimal offset;

    @NotNull
    private final BigDecimal coefficient;

    @NotNull
    private final int precision;

    public CoefficientOffset(
            @NotNull final String itemId,
            @NotNull final String dataKey,
            @NotNull BigDecimal offset,
            @NotNull BigDecimal coefficient,
            final int precision) {

        if (StringUtils.isBlank(dataKey)) {
            throw new IllegalArgumentException("数据键不能为空白");
        }
        if (BigDecimal.ZERO.equals(coefficient)) {
            throw new IllegalArgumentException("系数不能为0");
        }
        if (precision < 0) {
            throw new IllegalArgumentException("小数精度不能为负数");
        }

        this.itemId = itemId;
        this.dataKey = dataKey;
        this.offset = offset;
        this.coefficient = coefficient;
        this.precision = precision;
    }

    @Nullable
    public BigDecimal compute(
            @NotNull final ImmutableMap<String, String> data) {

        final String valueString = data.get(dataKey);
        if (!NumberUtils.isNumber(valueString)) {
            return null;
        }

        final BigDecimal value = DataUtils.createBigDecimal(valueString);
        if (null == value) {
            return null;
        }
        return compute(value);
    }

    @NotNull
    public BigDecimal compute(@NotNull BigDecimal value) {
        // 输出 = ( 输入 / 系数 ) - 偏移量
        return value.divide(coefficient, RoundingMode.HALF_UP).subtract(offset);
    }

    /**
     * 数据项标识
     */
    @NotNull
    @Contract(pure = true)
    public String getItemId() {
        return itemId;
    }

    /**
     * 数据键
     */
    @NotNull
    @Contract(pure = true)
    public String getDataKey() {
        return dataKey;
    }

    /**
     * 系数
     */
    @NotNull
    @Contract(pure = true)
    public BigDecimal getCoefficient() {
        return coefficient;
    }

    /**
     * 偏移
     */
    @Contract(pure = true)
    public BigDecimal getOffset() {
        return offset;
    }

    /**
     * 精度
     *
     * @return
     */
    @Contract(pure = true)
    public int getPrecision() {
        return precision;
    }

    @NotNull
    @Override
    public String toString() {
        return JsonUtils.getInstance().toJson(this);
    }

}
                                                  