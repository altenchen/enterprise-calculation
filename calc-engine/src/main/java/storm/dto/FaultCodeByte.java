package storm.dto;

import org.jetbrains.annotations.NotNull;

/**
 * @author xzp
 * 故障码(按字节解析)
 */
public final class FaultCodeByte {

    /**
     * 异常码ID(正常码Id就是故障码Id)
     */
    public final String codeId;

    /**
     * 故障码ID
     */
    public final String faultId;

    /**
     * 异常码/正常码
     */
    public final String equalCode;

    /**
     * 告警级别, 正常码都是0, 异常码按配置来
     */
    public final int alarmLevel;

    /**
     * 故障码类型, 0-正常码, 1-异常码
     */
    public final int type;

    public FaultCodeByte(
        @NotNull String codeId,
        @NotNull String code,
        int alarmLevel,
        int type,
        String faultId) {

        this.codeId = codeId;
        this.equalCode = code;
        this.alarmLevel = alarmLevel;
        this.type = type;
        this.faultId = faultId;
    }
}
                                                  