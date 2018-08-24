package storm.dto.alarm;

/**
 * @author wza
 * 偏移系数自定义数据项
 */
public class CoefficientOffset {

    /**
     * 数据键
     */
    public final String dataKey;

    /**
     * 数据项类型, 0-数值 1-数组
     */
    public final int dataType;

    /**
     * 系数
     */
    public final double coefficient;

    /**
     * 偏移值
     */
    public final double offset;

    /**
     * 定义数据项来源, 0-全局 1-用户
     */
    public final int defineSource;

    public CoefficientOffset(String dataKey, int dataType, double coefficient, double offset) {
        this(dataKey, dataType, coefficient, offset, 0);
    }

    public CoefficientOffset(String dataKey, int dataType, double coefficient, double offset, int defineSource) {
        this.dataKey = dataKey;
        this.dataType = dataType;
        this.coefficient = coefficient;
        this.offset = offset;
        this.defineSource = defineSource;
    }

    public boolean isArray() {
        return 1 == dataType;
    }

    public boolean isNumber() {
        return 0 == dataType;
    }

    public boolean isGlobal() {
        return 0 == defineSource;
    }

    public boolean isCustom() {
        return 1 == defineSource;
    }
}
                                                  