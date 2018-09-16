package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.function.BiFunction;
import java.util.function.Function;
import storm.util.function.TeFunction;

/**
 * @author 徐志鹏
 * 平台报警规则
 */
@SuppressWarnings("unused")
public class EarlyWarn {

    private static final Logger LOG = LoggerFactory.getLogger(EarlyWarn.class);

    /**
     * 规则Id
     */
    public final String ruleId;

    // region 组织属性

    /**
     * 车辆类型Id
     */
    public final String vehicleModelId;

    /**
     * 父规则Id
     */
    public final String parentRuleId;

    /**
     * 同车型子规则
     */
    public ImmutableMap<String, EarlyWarn> earlyWarns;

    /**
     * 特定车型子规则, 只适用于通用规则.
     */
    public ImmutableMap<String, ImmutableSet<EarlyWarn>> vehicleModelChildren;

    // endregion 组织属性

    // region 计算属性

    // region 数值运算

    /**
     * 左一数据项
     */
    public final String left1DataKey;

    /**
     * 左一数据使用之前的值
     */
    public final boolean left1UsePrev;

    /**
     * 左二数据项
     */
    public final String left2DataKey;

    /**
     * 左二数据使用之前的值
     */
    public final boolean left2UsePrev;

    /**
     * 数值运算表达式
     */
    public final String leftExpression;

    // endregion 数值运算

    // region 逻辑运算

    /**
     * 右一值
     */
    public final double right1Value;

    /**
     * 右二值
     */
    public double right2Value;

    /**
     * 逻辑运算表达式
     */
    public final String middleExpression;

    // endregion 逻辑运算

    /**
     * 运算表达式
     */
    public final BiFunction<
        ImmutableMap<String, String>,
        ImmutableMap<String, String>,
        Boolean> function;

    // endregion 计算属性

    // region 约定属性

    /**
     * 告警级别, 对于告警级别为0的规则, 使用报文中最高报警级别作为当前规则的告警级别.
     */
    public final int level;

    // endregion 约定属性

    // region 描述属性

    /**
     * 规则名称, HBase 需要
     */
    public final String ruleName;

    // endregion 描述属性

    EarlyWarn(
        @NotNull final String ruleId,
        @Nullable final String vehicleModelId,
        @Nullable final String parentRuleId,
        @NotNull final String left1DataKey,
        final boolean left1UsePrev,
        @Nullable final String left2DataKey,
        final boolean left2UsePrev,
        @Nullable final String leftExpression,
        final double right1Value,
        final double right2Value,
        @NotNull final String middleExpression,
        final BiFunction<ImmutableMap<String, String>, ImmutableMap<String, String>, Boolean> function,
        final int level,
        @Nullable final String ruleName) {

        this.ruleId = ruleId;
        this.vehicleModelId = vehicleModelId;
        this.parentRuleId = parentRuleId;

        this.left1DataKey = left1DataKey;
        this.left1UsePrev = left1UsePrev;
        this.left2DataKey = left2DataKey;
        this.left2UsePrev = left2UsePrev;
        this.leftExpression = leftExpression;

        this.right1Value = right1Value;
        this.right2Value = right2Value;
        this.middleExpression = middleExpression;

        this.function = function;

        this.level = level;

        this.ruleName = ruleName;
    }

    /**
     * 计算平台报警规则,
     * @param data 当前数据集
     * @param cache 之前有效数据集
     * @return true 表示符合规则, false 表示不符合规则, null 表示需要的数据项不完整
     */
    public Boolean compute(
        @Nullable final ImmutableMap<String, String> data,
        @Nullable final ImmutableMap<String, String> cache) {

        return function.apply(data, cache);
    }

    // region 构建函数

    /**
     * 构建平台报警函数, 如果构建参数不符合约束, 则返回 null .
     *
     * @param ruleId 平台报警规则标识, 用于打印日志.
     * @param ruleName 平台报警规则名称, 用于打印日志.
     * @param arithmeticExpression 算术运算表达式标记
     * @param left1DataKey 左一数据项键
     * @param left1UsePrev 左一数据项使用当前有效值还是上一次有效值
     * @param left2DataKey 左二数据项键
     * @param left2UsePrev 左二数据项使用当前有效值还是上一次有效值
     * @param logicExpression 逻辑运算表达式标记
     * @param right1Value 右一值
     * @param right2Value 右二值
     * @return 平台报警函数, 只需传入车辆当前数据项和上一次有效数据项, 即可计算结果,
     * true 表示符合规则, false 表示不符合规则, null 表示需要的数据项不完整
     */
    @Nullable
    public static BiFunction<
        ImmutableMap<String, String>,
        ImmutableMap<String, String>,
        Boolean> buildExpression(

        @NotNull final String ruleId,
        @Nullable final String ruleName,
        @Nullable final String arithmeticExpression,
        @NotNull final String left1DataKey,
        final boolean left1UsePrev,
        @Nullable final String left2DataKey,
        final boolean left2UsePrev,
        @NotNull final String logicExpression,
        @NotNull final String right1Value,
        @Nullable final String right2Value) {

        final BiFunction<
            ImmutableMap<String, String>,
            ImmutableMap<String, String>,
            BigDecimal> arithmeticFunction =
            buildArithmeticExpression(
                ruleId,
                ruleName,
                arithmeticExpression,
                left1DataKey,
                left1UsePrev,
                left2DataKey,
                left2UsePrev);


        if(null == arithmeticFunction) {
            return null;
        }

        final Function<BigDecimal, Boolean> logicFunction =
            buildLogicExpression(
                ruleId,
                ruleName,
                logicExpression,
                right1Value,
                right2Value);

        if (null == logicFunction) {
            return null;
        }

        return (data, cache) -> logicFunction.apply(
            arithmeticFunction.apply(data, cache));
    }

    /**
     * 构建算术运算函数
     * @param ruleId 平台报警规则标识, 用于打印日志.
     * @param ruleName 平台报警规则名称, 用于打印日志.
     * @param arithmeticExpression 算术运算表达式标记
     * @param left1DataKey 左一数据项键
     * @param left1UsePrev 左一数据项使用当前有效值还是上一次有效值
     * @param left2DataKey 左二数据项键
     * @param left2UsePrev 左二数据项使用当前有效值还是上一次有效值
     * @return 逻辑运算函数, 输入为实时数据获取器和缓存(上一次)数据获取器, 获取器输入为数据项的键, 输出为数据项的有效值, 如果没有有效值, 则输出 null.
     */
    @Nullable
    private static BiFunction<
        ImmutableMap<String, String>,
        ImmutableMap<String, String>,
        BigDecimal> buildArithmeticExpression(

        @NotNull final String ruleId,
        @Nullable final String ruleName,
        @Nullable final String arithmeticExpression,
        @NotNull final String left1DataKey,
        final boolean left1UsePrev,
        @Nullable final String left2DataKey,
        final boolean left2UsePrev) {

        if (StringUtils.isBlank(left1DataKey)) {
            LOG.error("平台报警规则[{}][{}]左一键空白, 无法构建算术运算函数.", ruleId, ruleName);
            return null;
        }

        final BiFunction<
            ImmutableMap<String, String>,
            ImmutableMap<String, String>,
            BigDecimal> firstGetter = buildDataGetter(left1UsePrev, left1DataKey);

        final String none = "0";
        if(null == arithmeticExpression || none.equals(arithmeticExpression)) {
            // 没有算术运算, 直接使用左一值.
            return firstGetter;
        } else {

            if (StringUtils.isBlank(left2DataKey)) {
                LOG.error("平台报警规则[{}][{}]左二键空白, 无法构建算术运算函数.", ruleId, ruleName);
                return null;
            }

            final BiFunction<
                ImmutableMap<String, String>,
                ImmutableMap<String, String>,
                BigDecimal> secondGetter = buildDataGetter(left2UsePrev, left2DataKey);

            final TeFunction<
                ImmutableMap<String, String>,
                ImmutableMap<String, String>,
                BiFunction<BigDecimal, BigDecimal, BigDecimal>,
                BigDecimal> valueFilter = buildValueFilter(firstGetter, secondGetter);

            return buildArithmeticExpression(arithmeticExpression, valueFilter, ruleId, ruleName);
        }
    }

    @Nullable
    private static BiFunction<
        ImmutableMap<String, String>,
        ImmutableMap<String, String>,
        BigDecimal> buildArithmeticExpression(
            @NotNull final String arithmeticExpression,
            final TeFunction<
                ImmutableMap<String, String>,
                ImmutableMap<String, String>,
                BiFunction<BigDecimal, BigDecimal, BigDecimal>,
                BigDecimal> valueFilter,
            final @NotNull String ruleId,
            final @Nullable String ruleName) {

        switch (arithmeticExpression) {
            // L1 + L2
            case "1": {
                return (data, cache) -> valueFilter.apply(
                    data,
                    cache,
                    BigDecimal::add);
            }
            // L1 - L2
            case "2":{
                return (dataGetter, cacheGetter) -> valueFilter.apply(
                    dataGetter,
                    cacheGetter,
                    BigDecimal::subtract);
            }
            // L1 * L2
            case "3": {
                return (dataGetter, cacheGetter) -> valueFilter.apply(
                    dataGetter,
                    cacheGetter,
                    BigDecimal::multiply);
            }
            // L1 / L2
            case "4": {
                return (dataGetter, cacheGetter) -> valueFilter.apply(
                    dataGetter,
                    cacheGetter,
                    BigDecimal::divide);
            }
            default:
                LOG.error("平台报警规则[{}][{}]未识别的数值运算表达式[{}]", ruleId, ruleName, arithmeticExpression);
                return null;
        }
    }

    /**
     * 构建逻辑运算函数
     * @param ruleId 平台报警规则标识, 用于打印日志.
     * @param ruleName 平台报警规则名称, 用于打印日志.
     * @param logicExpression 逻辑运算表达式标记
     * @param right1Value 右一值
     * @param right2Value 右二值
     * @return 逻辑运算函数, 输入为左值且输入不能为空.
     */
    @Nullable
    private static Function<BigDecimal, Boolean> buildLogicExpression(
        @NotNull final String ruleId,
        @Nullable final String ruleName,
        @NotNull final String logicExpression,
        @NotNull final String right1Value,
        @Nullable final String right2Value) {

        final BigDecimal first = createBigDecimal(right1Value);
        if(null == first) {
            LOG.error("平台报警规则[{}][{}]右一值无效, 无法构建逻辑运算函数.", ruleId, ruleName);
            return null;
        }

        switch (logicExpression) {
            case "0": {
                LOG.error("平台报警规则[{}][{}]空的逻辑运算表达式[{}]", ruleId, ruleName, logicExpression);
                return null;
            }
            // L = R1
            case "1": {
                return first::equals;
            }
            // L < R1
            case "2": {
                return leftValue -> leftValue.compareTo(first) < 0;
            }
            // L <= R1
            case "3": {
                return leftValue -> leftValue.compareTo(first) <= 0;
            }
            // L > R1
            case "4": {
                return leftValue -> leftValue.compareTo(first) > 0;
            }
            // L >= R1
            case "5": {
                return leftValue -> leftValue.compareTo(first) >= 0;
            }
            // L ∈ (R1, R2)
            case "6": {
                final BigDecimal second = createBigDecimal(right2Value);
                if (null == second) {
                    LOG.warn("平台报警规则[{}][{}]右二值无效, 无法构建数值区间逻辑表达式: L ∈ (R1, R2)", ruleId, ruleName);
                    return null;
                }
                return leftValue -> (0 < leftValue.compareTo(first)) && (leftValue.compareTo(second) < 0);
            }
            // L ∈ [R1, R2)
            case "7": {
                final BigDecimal second = createBigDecimal(right2Value);
                if (null == second) {
                    LOG.warn("平台报警规则[{}][{}]右二值无效, 无法构建数值区间逻辑表达式: L ∈ [R1, R2)", ruleId, ruleName);
                    return null;
                }
                return leftValue -> (0 <= leftValue.compareTo(first)) && (leftValue.compareTo(second) < 0);
            }
            // L ∈ (R1, R2]
            case "8": {
                final BigDecimal second = createBigDecimal(right2Value);
                if (null == second) {
                    LOG.warn("平台报警规则[{}][{}]右二值无效, 无法构建数值区间逻辑表达式: L ∈ (R1, R2]", ruleId, ruleName);
                    return null;
                }
                return leftValue -> (0 < leftValue.compareTo(first)) && (leftValue.compareTo(second) <= 0);
            }
            // L ∈ [R1, R2]
            case "9": {
                final BigDecimal second = createBigDecimal(right2Value);
                if (null == second) {
                    LOG.warn("平台报警规则[{}][{}]右二值无效, 无法构建数值区间逻辑表达式: L ∈ [R1, R2]", ruleId, ruleName);
                    return null;
                }
                return leftValue -> (0 <= leftValue.compareTo(first)) && (leftValue.compareTo(second) <= 0);
            }
            default: {
                LOG.warn("平台报警规则[{}][{}]未识别的逻辑运算表达式[{}]", ruleId, ruleName, logicExpression);
                return null;
            }
        }
    }

    @Nullable
    private static BigDecimal createBigDecimal(
        @Nullable final String value) {

        try
        {
            return NumberUtils.createBigDecimal(value);
        } catch (final Exception e) {
            LOG.error("转换字符串[{}]到BigDecimal异常", value);
            LOG.error("转换字符串到BigDecimal异常", e);
            return null;
        }
    }

    @NotNull
    @Contract(pure = true)
    private static BiFunction<
        ImmutableMap<String, String>,
        ImmutableMap<String, String>,
        BigDecimal> buildDataGetter(
            final boolean isUsePrev,
            @NotNull final String dataKey
    ) {
        if(isUsePrev) {
            return (data, cache) -> createBigDecimal(cache.get(dataKey));
        } else {
            return (data, cache) -> createBigDecimal(data.get(dataKey));
        }
    }

    @Contract(pure = true)
    @NotNull
    private static TeFunction<
        ImmutableMap<String, String>,
        ImmutableMap<String, String>,
        BiFunction<BigDecimal, BigDecimal, BigDecimal>,
        BigDecimal> buildValueFilter(

        final BiFunction<
            ImmutableMap<String, String>,
            ImmutableMap<String, String>,
            BigDecimal> firstGetter,
        final BiFunction<
            ImmutableMap<String, String>,
            ImmutableMap<String, String>,
            BigDecimal> secondGetter) {

        return (data, cache, function) -> {
            final BigDecimal first = firstGetter.apply(data, cache);
            if (null == first) {
                return null;
            }
            final BigDecimal second = secondGetter.apply(data, cache);
            if (null == second) {
                return null;
            }
            return function.apply(first, second);
        };
    }

    // endregion 构建函数
}
