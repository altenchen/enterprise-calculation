package storm.protocol;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-06-05
 * @description: 终端注册消息
 */
public final class SUBMIT_LOGIN {
    /**
     * 车辆ID, 车辆在平台中唯一编码
     */
    public static final String VehicleId = "VID";

    /**
     * 车辆类型, 预警用于匹配约束条件
     */
    public static final String VehicleType = "VTYPE";

    /**
     * 车机类型, 接入标识_协议种类_协议版本, 可通过CommandUtils工具类解析.
     * 接入标识:1.前置机接入 2.平台转发接入
     * 协议种类:1.国标协议 2.地标协议
     * 协议版本:1.0.0
     */
    public static final String CarType = "CTYPE";

    /**
     * 注册结果, 0-注册成功, 1-注册失败
     */
    public static final String LoginResult = "RESULT";

    /**
     * 注册时间, 时间格式YYYYmmddHHMMSS
     */
    public static final String LoginTime = "1001";

    /**
     * 注册流水号, 从1开始循环累加，最大值为65534
     */
    public static final String LoginSerial = "1002";

    /**
     * 车牌号, Bsse64(电动汽车号牌，GBK编码)
     */
    public static final String CarLicenseNnumber = "1003";

    /**
     * 车载终端编号, Base64(厂商代码)|Base64(终端批号)|Base64(流水号)
     */
    public static final String TerminaBoxId = "1004";

    /**
     * 车辆动力类型, 1-发动机，2-纯电动车，3-混合动力
     */
    public static final String VehiclePowerType = "1005";

    /**
     * 蓄电池包总数, 有效值范围：1～252
     */
    public static final String BatteryPackAmount = "1006";

    /**
     * 蓄电池代码列表, 列表使用“|”分隔符
     * base64(序号1_厂商代码1_电池类型1_额定能量1_额定电压1_生产日期1_流水号1)|base64(序号2_厂商代码2_电池类型2_额定能量2_额定电压2_生产日期2_流水号2)
     * 可通过BatteryInfo类来定位字符串拆分后的蓄电池代码
     */
    public static final String BatteryInfoList = "1007";

    /**
     * 发动机编码, Base64(唯一编码)
     */
    public static final String EngineId = "1008";

    /**
     * 燃油种类, 1-汽油发动机，2-柴油发动机，3-其他类型发动机
     */
    public static final String FuelType = "1009";

    /**
     * 最大输出功率, 有效值范围：1～252（KW）
     */
    public static final String MaxPowerOutput = "1010";

    /**
     * 最大输出转矩, 有效值范围：1～252（N*m）
     */
    public static final String MaxTorque = "1011";

    /**
     * 是否注册成功
     * @param loginResult 注册结果
     * @return 是否注册成功
     */
    @Contract(value = "null -> false", pure = true)
    public static boolean isLoginSucess(String loginResult) {
        return "0".equals(loginResult);
    }

    /**
     * 是否注册失败
     * @param loginResult 注册结果
     * @return 是否注册失败
     */
    @Contract(value = "null -> false", pure = true)
    public static boolean isLoginFailure(String loginResult) {
        return "1".equals(loginResult);
    }
}
