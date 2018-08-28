package storm.dto.alarm;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * @author wza
 * 预警规则获取
 */
public class EarlyWarn implements Serializable {

    private static final long serialVersionUID = 1999980001L;

    /**
     * 规则Id
     */
    public String ruleId;

    /**
     * 规则名称
     */
    public String ruleName;

    /**
     * 车辆类型Id
     */
    public String vehicleModelId;

    /**
     * 告警级别
     */
    public int levels;

    /**
     * 依赖Id
     */
    public String dependId;

    /**
     * 左一数据项
     */
    public String left1DataKey;

    /**
     * 左表达式
     */
    public String leftExpression;

    /**
     * 左二数据项
     */
    public String left2DataKey;

    /**
     * 中间表达式
     */
    public String middleExpression;

    /**
     * 右一值
     */
    public double right1Value;

    /**
     * 右二值
     */
    public double right2Value;

    /**
     * 是否适用所有车型
     */
    public boolean isAllCommon;

    // region 暂时不生效，等前端变业务

    /**
     * 依赖规则
     */
    public List<EarlyWarn> earlyWarns;

    /**
     * 连续多少次开始结束报警
     */
    public int judgeCount = 10;

    /**
     * 连续发生多少时间开始结束报警
     */
    public long judgeTime = -1;

    // endregion 暂时不生效，等前端变业务

    public EarlyWarn(
        final String ruleId,
        final String ruleName,
        final String vehicleModelId,
        final int levels,
        final String dependId,
        final String left1DataKey,
        final String leftExpression,
        final String left2DataKey,
        final String middleExpression,
        final double right1Value,
        final double right2Value) {

        this.ruleId = ruleId;
        this.ruleName = ruleName;
        this.vehicleModelId = vehicleModelId;
        this.levels = levels;
        this.dependId = dependId;
        this.left1DataKey = left1DataKey;
        this.leftExpression = leftExpression;
        this.left2DataKey = left2DataKey;
        this.middleExpression = middleExpression;
        this.right1Value = right1Value;
        this.right2Value = right2Value;

        setCommon(vehicleModelId);
    }

    /**
     * 判断是否适用所有车型
     * @param vehModelId
     */
    void setCommon(final String vehModelId) {
        if (StringUtils.isBlank(vehModelId)
            || "ALL".equals(vehModelId.trim())) {

            this.isAllCommon = true;

        } else {

            this.isAllCommon = false;
        }
    }

    // region 暂时不生效，等前端变业务

    /**
     * 此方法暂时不用，等前端业务变了以后再使用
     *
     * @param common
     * @param commonCount
     * @param count
     */
    void setJudgeCondition(boolean common, int commonCount, int count) {
        if (common) {
            this.judgeCount = commonCount;
        } else {
            this.judgeCount = count;
        }
    }

    void setDependWarns(List<EarlyWarn> earlyWarns) {
        this.earlyWarns = earlyWarns;
    }

    void addDependWarns(EarlyWarn warn) {
        if (null == warn) {
            return;
        }

        if (null == earlyWarns) {
            earlyWarns = new LinkedList<>();
        }

        if (!earlyWarns.contains(warn)) {
            earlyWarns.add(warn);
        }
    }

    // endregion 暂时不生效，等前端变业务

}
