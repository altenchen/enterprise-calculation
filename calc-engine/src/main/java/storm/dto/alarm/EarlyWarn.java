package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 徐志鹏
 * 平台报警规则
 */
public class EarlyWarn {

    private static final Logger LOG = LoggerFactory.getLogger(EarlyWarn.class);

    /**
     * 规则Id
     */
    public final String ruleId;

    // region 描述属性

    /**
     * 规则名称, HBase 需要
     */
    public final String ruleName;

    /**
     * 告警级别
     */
    public final int level;

    // endregion 描述属性

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
    public ImmutableSet<EarlyWarn> earlyWarns;

    /**
     * 特定车型子规则, 只适用于通用规则.
     */
    public ImmutableMap<String, ImmutableSet<EarlyWarn>> vehicleModelChildren;

    // endregion 组织属性

    // region 时间属性

    // 开始时间
    // 结束时间

    // endregion 时间属性

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

    // endregion 计算属性

    public EarlyWarn(
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
        @Nullable final String ruleName,
        final int level) {

        this.ruleId = ruleId;
        this.vehicleModelId = vehicleModelId;
        this.parentRuleId = parentRuleId;
        this.left1DataKey = left1DataKey;
        this.left1UsePrev = left1UsePrev;
        this.leftExpression = leftExpression;
        this.left2DataKey = left2DataKey;
        this.left2UsePrev = left2UsePrev;
        this.middleExpression = middleExpression;
        this.right1Value = right1Value;
        this.right2Value = right2Value;
        this.ruleName = ruleName;
        this.level = level;
    }

    @Nullable
    public static String parseLeftExpression(final int leftExpression) {
        switch (leftExpression) {
            case 0:
                return null;
            case 1:
                return "+";
            case 2:
                return "-";
            case 3:
                return "*";
            case 4:
                return "/";
            default:
                LOG.error("未定义的数值运算表达式[{}]", leftExpression);
                return null;
        }
    }

    @Nullable
    public static String parseMiddleExpression(final int middleExpression) {
        switch (middleExpression) {
            case 0:
                return null;
            case 1:
                return "=";
            case 2:
                return "<";
            case 3:
                return "<=";
            case 4:
                return ">";
            case 5:
                return ">=";
            case 6:
                return "< <";
            case 7:
                return "<= <";
            case 8:
                return "< <=";
            case 9:
                return "<= <=";
            default:
                LOG.error("未定义的逻辑运算表达式[{}]", middleExpression);
                return null;
        }
    }
}
