package storm.dto;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 故障类型按位解析(单个位)
 * @author: xzp
 * @date: 2018-06-24
 * @description:
 */
public class FaultTypeSingleBit {

    /**
     * 故障码Id
     */
    public final String faultId;

    /**
     * 故障种类(内部协议标号)
     */
    public final String faultType;

    /**
     * 解析方式, 1-按字节解析, 2-按位解析, 当前方案固化为按位解析.
     */
    public final String analyzeType;

    /**
     * 故障码规则, <车型, <异常码Id, 异常码>>
     */
    @NotNull
    public final Map<String, Map<String, ExceptionSingleBit>> vehExceptions = new HashMap<>();

    public FaultTypeSingleBit(String faultId, String faultType, String analyzeType) {
        this.faultId = faultId;
        this.faultType = faultType;
        this.analyzeType = analyzeType;
    }

}
