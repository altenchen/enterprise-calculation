package storm.system;

/**
 * @author: xzp
 * @date: 2018-06-06
 * @description: Storm流中数据字典的键
 *
 * 数字下标的采用 _索引数字_易读命名 的格式
 */
public final class DataKey {

    /**
     * 车辆ID, 车辆在平台中唯一编码
     */
    public static final String VEHICLE_ID = "VID";

    /**
     * VIN
     */
    public static final String VEHICLE_NUMBER = "VIN";

    /**
     * 时间
     */
    public static final String TIME = "TIME";

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
     * 采集时间
     */
    public static final String _2000_COLLECT_TIME = "2000";

    /**
     * 采集时间
     */
    public static final String _3101_COLLECT_TIME = "3101";

    /**
     * 状态标志
     */
    public static final String _3110_STATUS_FLAGS = "3110";
}
