package storm.dto;

import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.List;

/**
 * @author xzp
 * 故障码告警规则(按字节解析)
 */
public final class FaultCodeByteRule {

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
    private final List<FaultCodeByte> faultCodes = new LinkedList<>();

    public FaultCodeByteRule(String faultId, String faultType) {
        this.faultId = faultId;
        this.faultType = faultType;
    }

    /**
     * 添加故障码
     * @param faultCode 故障码
     */
    public void addFaultCode(@NotNull FaultCodeByte faultCode) {
        faultCodes.add(faultCode);
    }

    /**
     * 获取故障码集合
     */
    public Iterable<FaultCodeByte> getFaultCodes() {
        return faultCodes;
    }
}
