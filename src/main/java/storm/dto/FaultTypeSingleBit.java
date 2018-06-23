package storm.dto;

import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.TreeMap;

/**
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
    public final Map<String, Map<String, ExceptionSingleBit>> vehExceptions = new TreeMap<>();

    public FaultTypeSingleBit(String faultId, String faultType, String analyzeType) {
        this.faultId = faultId;
        this.faultType = faultType;
        this.analyzeType = analyzeType;
    }

    /**
     * 获取车型的异常字典, 适用于初始化, 对于不存在的车型, 会自动创建空字典.
     * @param vehicleModel 车型
     * @return 车型的异常字典
     */
    public final synchronized Map<String, ExceptionSingleBit> ensureVehExceptions(String vehicleModel) {
        if(!vehExceptions.containsKey(vehicleModel)) {
            vehExceptions.put(vehicleModel, new TreeMap<>());
        }
        return vehExceptions.get(vehicleModel);
    }
}
