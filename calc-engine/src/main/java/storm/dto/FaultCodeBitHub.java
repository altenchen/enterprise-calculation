package storm.dto;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author: xzp
 * @date: 2018-06-24
 * @description:
 */
public final class FaultCodeBitHub {
    private static final Logger LOGGER = LoggerFactory.getLogger(FaultCodeBitHub.class);

    /**
     * 车辆关联的故障码 Map<vid, Map<faultType, fault>>
     */
    @NotNull
    private final Map<String, Map<String, FaultTypeBit>> vehRules = new TreeMap<>();

    /**
     * @param faultId 故障码ID, 全局唯一
     * @param faultType 故障种类(内部协议标号), 对于同一车型最多只有1条, 后续的忽略.
     * @param analyzeType 解析方式, 1-按字节解析, 2-按位解析
     * @param vehModelIds 适用车型, 多个车型之间用逗号分隔, 如果是空白字符串, 表示作为默认值.
     * @param paramLength 按位解析时有效 - 位长度
     */
    public void appendFaultException(
        String faultId,
        String faultType,
        String analyzeType,
        String vehModelIds,
        String paramLength) {

    }
}
