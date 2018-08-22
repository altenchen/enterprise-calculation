package storm.system;

/**
 * 通知类型
 * @author: xzp
 */
public final class NoticeType {

    /**
     * 无 CAN 通知
     */
    public static final String NO_CAN_VEH = "NO_CAN_VEH";

    /**
     * 未定位通知
     */
    public static final String NO_POSITION_VEH = "NO_POSITION_VEH";

    /**
     * SOC 过低通知
     */
    public static final String SOC_ALARM = "SOC_ALARM";

    /**
     * 车辆锁止状态通知
     */
    public static final String VEH_LOCK_STATUS_CHANGE = "VEH_LOCK_STATUS_CHANGE";
}
