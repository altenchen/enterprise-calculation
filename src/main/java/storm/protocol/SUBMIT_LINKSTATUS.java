package storm.protocol;

import org.jetbrains.annotations.Contract;

/**
 * @author: xzp
 * @date: 2018-06-05
 * @description: 链接状态通知
 */
public final class SUBMIT_LINKSTATUS {
    /**
     * 车辆ID, 车辆在平台中唯一编码
     */
    public static final String VEHICLE_ID = "VID";

    /**
     * 车辆类型, 预警用于匹配约束条件
     */
    public static final String VEHICLE_TYPE = "VTYPE";

    /**
     * 车机类型, 接入标识_协议种类_协议版本, 可通过CommandUtils工具类解析.
     * 接入标识:1.前置机接入 2.平台转发接入
     * 协议种类:1.国标协议 2.地标协议
     * 协议版本:1.0.0
     */
    public static final String CAR_TYPE = "CTYPE";

    /**
     * 通知时间, 格式'YYYYmmddHHMMSS'
     */
    public static final String NOTICE_TIME = "TIME";

    /**
     * 通知类型, 1-上线, 2-心跳, 3-离线
     */
    public static final String LINK_TYPE = "TYPE";

    /**
     * 是否上线通知
     * @param linkType 通知类型
     * @return 是否上线
     */
    @Contract(value = "null -> false", pure = true)
    public static boolean isOnlineNotice(String linkType) {
        return "1".equals(linkType);
    }

    /**
     * 是否心跳通知
     * @param linkType 通知类型
     * @return 是否心跳通知
     */
    @Contract(value = "null -> false", pure = true)
    public static boolean isHeartbeatNotice(String linkType) {
        return "2".equals(linkType);
    }

    /**
     * 是否离线通知
     * @param linkType 通知类型
     * @return 是否离线通知
     */
    @Contract(value = "null -> false", pure = true)
    public static boolean isOfflineNotice(String linkType) {
        return "3".equals(linkType);
    }
}
