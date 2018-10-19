package storm.system;

/**
 * 通知类型
 * @author: xzp
 */
public final class NoticeType {

    /**
     * 异常用车通知
     */
    public static final String ABNORMAL_USE_VEH = "ABNORMAL_USE_VEH";

    /**
     * 距离最近配电车通知
     */
    public static final String CHARGE_CAR_NOTICE = "CHARGE_CAR_NOTICE";

    /**
     * 飞机通知
     */
    public static final String FLY_RECORD = "FLY_RECORD";

    /**
     * 里程跳变通知
     */
    public static final String HOP_MILE = "HOP_MILE";

    /**
     * 长期离线车辆通知
     */
    public static final String IDLE_VEH = "IDLE_VEH";

    /**
     * 点火熄火通知
     */
    public static final String IGNITE_SHUT_MESSAGE = "IGNITE_SHUT_MESSAGE";

    /**
     * 无 CAN 通知
     */
    public static final String NO_CAN_VEH = "NO_CAN_VEH";

    /**
     * 未定位通知
     */
    public static final String NO_POSITION_VEH = "NO_POSITION_VEH";

    /**
     * 上下线通知
     */
    public static final String ON_OFF = "ON_OFF";

    /**
     * 上下线里程通知
     */
    public static final String ON_OFF_MILE = "ON_OFF_MILE";

    /**
     * SOC 过低通知
     */
    public static final String SOC_ALARM = "SOC_ALARM";

    /**
     * SOC 过高通知
     */
    public static final String SOC_HIGH_ALARM = "SOC_HIGH_ALARM";

    /**
     * 时间异常通知
     */
    public static final String TIME_EXCEPTION_VEH = "TIME_EXCEPTION_VEH";

    /**
     * 车辆锁止状态通知
     */
    public static final String VEH_LOCK_STATUS_CHANGE = "VEH_LOCK_STATUS_CHANGE";
}
