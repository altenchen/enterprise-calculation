package storm.dto;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
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

    public FaultCodeByteRule(String faultId, String faultType) {
        this.faultId = faultId;
        this.faultType = faultType;
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
}
