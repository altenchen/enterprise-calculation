package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.DataUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.BiFunction;
import java.util.function.Function;

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
    public final String right1Value;

    /**
     * 右二值
     */
    public String right2Value;

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
     * 告警级别, 对于告警级别为 0 的规则, 使用报文中最高报警级别作为当前规则的告警级别.
     */
    public final int level;

    // endregion 约定属性

    // region 描述属性

    /**
     * 规则名称, 日志和 HBase 需要
     */
    public final String ruleName;

    // endregion 描述属性

    EarlyWarn(
        @NotNull final String ruleId,
        @Nullable final String ruleName,
        @NotNull final String left1DataKey,
        final boolean left1UsePrev,
        @Nullable final String left2DataKey,
        final boolean left2UsePrev,
        @Nullable final String leftExpression,
        @NotNull final String right1Value,
        @Nullable final String right2Value,
        @NotNull final String middleExpression,
        final BiFunction<ImmutableMap<String, String>, ImmutableMap<String, String>, Boolean> function,
        final int level,
        @Nullable final String vehicleModelId) {

        this.ruleId = ruleId;
        this.ruleName = ruleName;

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

        this.vehicleModelId = vehicleModelId;
    }

    /**
     * 计算平台报警规则
     *
     * @param data  当前数据集
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
     * @param ruleId               平台报警规则标识, 用于打印日志.
     * @param ruleName             平台报警规则名称, 用于打印日志.
     * @param left1DataKey         左一数据项键
     * @param left1UsePrev         左一数据项使用当前有效值还是上一次有效值
     * @param left2DataKey         左二数据项键
     * @param left2UsePrev         左二数据项使用当前有效值还是上一次有效值
     * @param arithmeticExpression 算术运算表达式标记
     * @param right1Value          右一值
     * @param right2Value          右二值
     * @param logicExpression      逻辑运算表达式标记
     * @return 平台报警函数, 只需传入车辆当前数据项和上一次有效数据项, 即可计算结果,
     * true 表示符合规则, false 表示不符合规则, null 表示需要的数据项不完整
     */
    @Nullable
    public static BiFunction<
        ImmutableMap<String, String>,
        ImmutableMap<String, String>,
        Boolean> buildFunction(

        @NotNull final String ruleId,
        @Nullable final String ruleName,
        @NotNull final String left1DataKey,
        final boolean left1UsePrev,
        @Nullable final String left2DataKey,
        final boolean left2UsePrev,
        @Nullable final String arithmeticExpression,
        @NotNull final String right1Value,
        @Nullable final String right2Value,
        @NotNull final String logicExpression) {

        final BiFunction<ImmutableMap<String, String>, ImmutableMap<String, String>, BigDecimal> arithmeticFunction =
            buildArithmeticFunction(
                ruleId,
                ruleName,
                left1DataKey,
                left1UsePrev,
                left2DataKey,
                left2UsePrev,
                arithmeticExpression);

        if (null == arithmeticFunction) {
            return null;
        }

        final Function<Function<Integer, BigDecimal>, Boolean> logicFunction =
            buildLogicFunction(
                ruleId,
                ruleName,
                logicExpression,
                right1Value,
                right2Value);

        if (null == logicFunction) {
            return null;
        }

        return (data, cache) -> {
            final BigDecimal arithmeticValue = arithmeticFunction.apply(data, cache);
            if(null == arithmeticValue) {
                return null;
            }
            return logicFunction.apply(scale -> arithmeticValue.setScale(scale, RoundingMode.HALF_UP));
        };
    }


    // region 构建数据摘取函数

    /**
     * 构建算术运算函数
     *
     * @param ruleId               平台报警规则标识, 用于打印日志.
     * @param ruleName             平台报警规则名称, 用于打印日志.
     * @param arithmeticExpression 算术运算表达式标记
     * @param left1DataKey         左一数据项键
     * @param left1UsePrev         左一数据项使用当前有效值还是上一次有效值
     * @param left2DataKey         左二数据项键
     * @param left2UsePrev         左二数据项使用当前有效值还是上一次有效值
     * @return 算术运算函数, 输入为实时数据获取器和缓存(上一次)数据获取器, 获取器输入为数据项的键, 输出为数据项的有效值, 如果没有有效值, 则输出 null.
     */
    @Nullable
    private static BiFunction<ImmutableMap<String, String>, ImmutableMap<String, String>, BigDecimal> buildArithmeticFunction(

        @NotNull final String ruleId,
        @Nullable final String ruleName,
        @NotNull final String left1DataKey,
        final boolean left1UsePrev,
        @Nullable final String left2DataKey,
        final boolean left2UsePrev,
        @Nullable final String arithmeticExpression) {

        if (StringUtils.isBlank(left1DataKey)) {
            LOG.error("平台报警规则[{}][{}]左一键空白, 无法构建算术运算函数.", ruleId, ruleName);
            return null;
        }

        final BiFunction<
            ImmutableMap<String, String>,
            ImmutableMap<String, String>,
            BigDecimal> firstGetter = buildDataGetter(left1DataKey, left1UsePrev);

        final String none = "0";
        if (StringUtils.isBlank(arithmeticExpression) || none.equals(arithmeticExpression)) {
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
                BigDecimal> secondGetter = buildDataGetter(left2DataKey, left2UsePrev);

            final BiFunction<BigDecimal, BigDecimal, BigDecimal> function =
                buildArithmeticFunction(ruleId, ruleName, arithmeticExpression);

            return (data, cache) -> function.apply(
                firstGetter.apply(data, cache),
                secondGetter.apply(data, cache));
        }
    }

    /**
     * 构建数据摘取函数
     * @param isUseCache 是否使用缓存的上一次有效值
     * @param dataKey 数据键
     * @return 数据摘取函数
     */
    @NotNull
    @Contract(pure = true)
    public static BiFunction<
        ImmutableMap<String, String>,
        ImmutableMap<String, String>,
        BigDecimal> buildDataGetter(
        @NotNull final String dataKey,
        final boolean isUseCache
    ) {
        return (data, cache) -> buildDataGetter(dataKey).apply(isUseCache ? cache : data);
    }

    @NotNull
    @Contract(pure = true)
    public static Function<
        ImmutableMap<String, String>,
        BigDecimal> buildDataGetter(
        @NotNull final String dataKey
    ) {
        return (map) -> {
            if(MapUtils.isNotEmpty(map)) {
                final String itemString = map.get(dataKey);
                if (StringUtils.isNotBlank(itemString)) {
                    final BigDecimal itemValue = DataUtils.createBigDecimal(itemString);
                    if(null != itemValue) {
                        final CoefficientOffset coefficientOffset = CoefficientOffsetGetter.getCoefficientOffset(dataKey);
                        if (null != coefficientOffset) {
                            return coefficientOffset.compute(itemValue);
                        }
                        return itemValue;
                    }
                }
            }
            return null;
        };
    }

    // endregion 构建数据摘取函数

    // region 构建算术运算函数

    /**
     * 构建算术运算函数
     *
     * @param ruleId 平台报警规则标识, 用于打印日志.
     * @param ruleName 平台报警规则名称, 用于打印日志.
     * @param arithmeticExpression 算术运算表达式标记
     * @return 逻辑运算函数, 输入为实时数据获取器和缓存(上一次)数据获取器, 获取器输入为数据项的键, 输出为数据项的有效值, 如果没有有效值, 则输出 null.
     */
    @Nullable
    public static BiFunction<BigDecimal, BigDecimal, BigDecimal> buildArithmeticFunction(
        @NotNull final String ruleId,
        @Nullable final String ruleName,
        @NotNull final String arithmeticExpression) {

        switch (arithmeticExpression) {
            // L1 + L2
            case "1": {
                return buildArithmeticFunctionFilter(
                    (left1Value, left2Value) -> left1Value.add(left2Value));
            }
            // L1 - L2
            case "2": {
                return buildArithmeticFunctionFilter(
                    (left1Value, left2Value) -> left1Value.subtract(left2Value));
            }
            // L1 * L2
            case "3": {
                return buildArithmeticFunctionFilter(
                    (left1Value, left2Value) -> left1Value.multiply(left2Value));
            }
            // L1 / L2
            case "4": {
                return buildArithmeticFunctionFilter(
                    (left1Value, left2Value) -> left1Value.divide(left2Value, Math.max(left1Value.scale(),left2Value.scale()), BigDecimal.ROUND_HALF_UP));
            }
            default:
                LOG.error("平台报警规则[{}][{}]未识别的数值运算表达式[{}]", ruleId, ruleName, arithmeticExpression);
                return null;
        }
    }

    @Contract(pure = true)
    @NotNull
    private static BiFunction<BigDecimal, BigDecimal, BigDecimal> buildArithmeticFunctionFilter(
        @NotNull final BiFunction<BigDecimal, BigDecimal, BigDecimal> arithmeticFunction
    ) {

        return (left1Value, left2Value) -> {
            if (null == left1Value) {
                return null;
            }
            if (null == left2Value) {
                return null;
            }
            return arithmeticFunction.apply(left1Value, left2Value);
        };
    }

    // endregion 构建算术运算函数

    // region 构建逻辑运算函数

    /**
     * 构建逻辑运算函数
     *
     * @param ruleId 平台报警规则标识, 用于打印日志.
     * @param ruleName 平台报警规则名称, 用于打印日志.
     * @param logicExpression 逻辑运算表达式标记
     * @param right1Value 右一值
     * @param right2Value 右二值
     * @return 逻辑运算函数, 输入为左值获取函数且输入不能为空.
     */
    @Nullable
    public static Function<Function<Integer, BigDecimal>, Boolean> buildLogicFunction(
        @NotNull final String ruleId,
        @Nullable final String ruleName,
        @NotNull final String logicExpression,
        @NotNull final String right1Value,
        @Nullable final String right2Value) {

        final BigDecimal first = DataUtils.createBigDecimal(right1Value);
        if (null == first) {
            LOG.error("平台报警规则[{}][{}]右一值不是小数, 无法构建逻辑运算函数.", ruleId, ruleName);
            return null;
        }

        switch (logicExpression) {
            // L = R1
            case "1": {
                return buildLogicFunctionFilter(
                    first.scale(),
                    leftValue -> leftValue.compareTo(first) == 0);
            }
            // L < R1
            case "2": {
                return buildLogicFunctionFilter(
                    first.scale(),
                    leftValue -> leftValue.compareTo(first) < 0);
            }
            // L <= R1
            case "3": {
                return buildLogicFunctionFilter(
                    first.scale(),
                    leftValue -> leftValue.compareTo(first) <= 0);
            }
            // L > R1
            case "4": {
                return buildLogicFunctionFilter(
                    first.scale(),
                    leftValue -> leftValue.compareTo(first) > 0);
            }
            // L >= R1
            case "5": {
                return buildLogicFunctionFilter(
                    first.scale(),
                    leftValue -> leftValue.compareTo(first) >= 0);
            }
            // L ∈ (R1, R2)
            case "6": {

                final BigDecimal second = DataUtils.createBigDecimal(right2Value);
                if (null == second) {
                    LOG.warn("平台报警规则[{}][{}]右二值无效, 无法构建数值区间逻辑表达式: L ∈ (R1, R2)", ruleId, ruleName);
                    return null;
                }
                final int maxScale = Math.max(first.scale(), second.scale());
                return buildLogicFunctionFilter(
                    maxScale,
                    leftValue -> (0 < leftValue.compareTo(first)) && (leftValue.compareTo(second) < 0));
            }
            // L ∈ [R1, R2)
            case "7": {

                final BigDecimal second = DataUtils.createBigDecimal(right2Value);
                if (null == second) {
                    LOG.warn("平台报警规则[{}][{}]右二值无效, 无法构建数值区间逻辑表达式: L ∈ [R1, R2)", ruleId, ruleName);
                    return null;
                }
                final int maxScale = Math.max(first.scale(), second.scale());
                return buildLogicFunctionFilter(
                    maxScale,
                    leftValue -> (0 <= leftValue.compareTo(first)) && (leftValue.compareTo(second) < 0));
            }
            // L ∈ (R1, R2]
            case "8": {

                final BigDecimal second = DataUtils.createBigDecimal(right2Value);
                if (null == second) {
                    LOG.warn("平台报警规则[{}][{}]右二值无效, 无法构建数值区间逻辑表达式: L ∈ (R1, R2]", ruleId, ruleName);
                    return null;
                }
                final int maxScale = Math.max(first.scale(), second.scale());
                return buildLogicFunctionFilter(
                    maxScale,
                    leftValue -> (0 < leftValue.compareTo(first)) && (leftValue.compareTo(second) <= 0));
            }
            // L ∈ [R1, R2]
            case "9": {

                final BigDecimal second = DataUtils.createBigDecimal(right2Value);
                if (null == second) {
                    LOG.warn("平台报警规则[{}][{}]右二值无效, 无法构建数值区间逻辑表达式: L ∈ [R1, R2]", ruleId, ruleName);
                    return null;
                }
                final int maxScale = Math.max(first.scale(), second.scale());
                return buildLogicFunctionFilter(
                    maxScale,
                    leftValue -> (0 <= leftValue.compareTo(first)) && (leftValue.compareTo(second) <= 0));
            }
            default: {
                LOG.warn("平台报警规则[{}][{}]未识别的逻辑运算表达式[{}]", ruleId, ruleName, logicExpression);
                return null;
            }
        }
    }

    @NotNull
    @Contract(pure = true)
    private static Function<Function<Integer, BigDecimal>, Boolean> buildLogicFunctionFilter(
        final int scale,
        @NotNull final Function<BigDecimal, Boolean> logicFunction
    ) {

        return arithmeticFunction ->
        {
            final BigDecimal leftValue = arithmeticFunction.apply(scale);
            if (null == leftValue) {
                return null;
            }
            return logicFunction.apply(leftValue);
        };
    }

    // endregion 构建逻辑运算函数

    // endregion 构建函数


    @Override
    public String toString() {
        return StringUtils.defaultIfEmpty(ruleName, super.toString());
    }
}
