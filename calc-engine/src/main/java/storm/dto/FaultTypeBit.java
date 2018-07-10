package storm.dto;

import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author: xzp
 * @date: 2018-06-22
 * @description: 故障码告警规则(按位解析)
 * [2018年6月24日] 未完成, 暂不使用, 简化为按1bit表示处理, 有时间再整.
 */
public final class FaultTypeBit {

    /**
     * 故障码Id
     */
    public final String id;

    /**
     * 故障种类(内部协议标号)
     */
    public final String faultType;

    /**
     * 解析方式, 1-按字节解析, 2-按位解析
     */
    public final String analyzeType;

    /**
     * 适用车型
     */
    public final String[] vehModelIds;

    /**
     * 故障码规则, <异常码Id, 异常码>
     */
    @NotNull
    private final Map<String, PartationBit> exceptions;

    public FaultTypeBit(
        String faultType,
        String analyzeType,
        String id, String[] vehModelIds,
        Map<String, PartationBit> exceptions) {

        this.faultType = faultType;
        this.analyzeType = analyzeType;
        this.id = id;
        this.vehModelIds = vehModelIds;
        this.exceptions = exceptions;
    }

    /**
     * @param codes 码值
     * @return 匹配的异常码
     */
    @NotNull
    public Map<String, PartationBit> processFrame(@NotNull final long[] codes) {
        final Map<String, PartationBit> result = new TreeMap<>();
        for (final PartationBit exception : exceptions.values()) {
            if(exception.equals(codes)) {
                result.put(exception.partitionId, exception);
            }
        }
        return result;
    }

    public Map<String, PartationBit> getExceptions() {
        return MapUtils.unmodifiableMap(exceptions);
    }
}
