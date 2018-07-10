package storm.protocol;

/**
 * @author: xzp
 * @date: 2018-06-05
 * @description: 私有协议5块组成部分索引
 * 消息结构：消息前缀 序列号 VIN码 命令标识 参数集。
 */
public final class ProtocolSeparator {

    /**
     * 消息前缀
     */
    public static final int PREFIX = 0;

    /**
     * 序列号
     */
    public static final int SERIAL_NO = 1;

    /**
     * VIN码
     */
    public static final int VIN = 2;

    /**
     * 命令标识
     */
    public static final int COMMAND_TYPE = 3;

    /**
     * 参数集
     */
    public static final int CONTENT = 4;
}
