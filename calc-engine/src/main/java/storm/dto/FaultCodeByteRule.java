package storm.dto;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 修改 [许智杰 2018-10-15]：
 *      添加vehModels字段，规则适用车型列表
 *      添加effective方法，判断车型是否使用该规则
 *
 * @author xzp
 * 故障码告警规则(按字节解析)
 */
public final class FaultCodeByteRule {
    private static final Logger logger = LoggerFactory.getLogger(FaultCodeByteRule.class);

    /**
     * 故障码ID
     */
    public final String faultId;

    /**
     * 故障类型
     */
    public final String faultType;

    /**
     * 异常码集合 + 正常码
     */
    private final Map<String, FaultCodeByte> faultCodes = new TreeMap<>();

    /**
     * 该告警规则所适用车型
     */
    private Set<String> vehModels = new HashSet<>(0);

    public FaultCodeByteRule(String faultId, String faultType, String[] modelNum) {
        this.faultId = faultId;
        this.faultType = faultType;
        Arrays.stream(modelNum).forEach(vehModels::add);
    }

    /**
     * 添加故障码
     * @param faultCode 故障码
     */
    public void addFaultCode(@NotNull FaultCodeByte faultCode) {
        if(faultCodes.containsKey(faultCode.codeId)) {
            logger.warn("重复的异常码[{}]", faultCode.codeId);
        }
        faultCodes.put(faultCode.codeId, faultCode);
    }

    /**
     * 获取故障码集合
     */
    public Collection<FaultCodeByte> getFaultCodes() {
        return faultCodes.values();
    }

    /**
     * 规则是否对该车型生效
     * @param vehModel 车型
     * @return
     */
    public boolean effective(String vehModel){
        if(StringUtils.isEmpty(vehModel)){
            return false;
        }
        return vehModels.contains(vehModel);
    }
}
