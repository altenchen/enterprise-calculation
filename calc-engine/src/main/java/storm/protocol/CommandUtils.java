package storm.protocol;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-06-05
 * @description: 命令辅助工具类
 */
public final class CommandUtils {

    /**
     * 拆分机车类型
     * 接入标识_协议种类_协议版本
     * 接入标识:1.前置机接入 2.平台转发接入
     * 协议种类:1.国标协议 2.地标协议
     * 协议版本:1.0.0
     * @param carType
     * @return [接入标识, 协议种类, 协议版本]
     */
    @Contract("null -> null")
    public static String[] splitCarType(@NotNull String carType) {

        return StringUtils.split(carType, "_");
    }

    /**
     * 是否前置机接入
     * @param datasource 接入标识
     * @return 是否前置机接入
     */
    @Contract(value = "null -> false", pure = true)
    public static boolean isFromFrontEndProcessor(String datasource) {
        return "1".equals(datasource);
    }

    /**
     * 是否平台转发接入
     * @param datasource 接入标识
     * @return 是否平台转发接入
     */
    @Contract(value = "null -> false", pure = true)
    public static boolean isFromPlatformTranspond(String datasource) {
        return "2".equals(datasource);
    }

    /**
     * 是否国标协议
     * @param protocol 协议种类
     * @return 是否国标协议
     */
    @Contract(value = "null -> false", pure = true)
    public static boolean isNationalProtocol(String protocol) {
        return "1".equals(protocol);
    }

    /**
     * 是否地标协议
     * @param protocol 协议种类
     * @return 是否地标协议
     */
    @Contract(value = "null -> false", pure = true)
    public static boolean isRegionalProtocol(String protocol) {
        return "2".equals(protocol);
    }
}
