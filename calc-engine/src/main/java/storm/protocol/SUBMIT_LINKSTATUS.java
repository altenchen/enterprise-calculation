package storm.protocol;

import org.jetbrains.annotations.Contract;
import storm.system.DataKey;

/**
 * @author: xzp
 * @date: 2018-06-05
 * @description: 链接状态通知
 */
public final class SUBMIT_LINKSTATUS {

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
