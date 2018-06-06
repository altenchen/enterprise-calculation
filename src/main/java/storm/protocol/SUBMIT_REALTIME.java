package storm.protocol;

/**
 * @author: xzp
 * @date: 2018-06-05
 * @description: 实时信息上报
 * 数字下标的采用 _索引数字_易读命名 的格式
 */
public final class SUBMIT_REALTIME {

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
     * 单体蓄电池总数
     */
    public static final String _2001 = "2001";

    /**
     * 动力蓄电池包总数
     */
    public static final String _2002 = "2002";

    /**
     * 单体蓄电池电压值列表
     */
    public static final String SINGLE_VOLT = "2003";

    /**
     * 蓄电池包温度探针总数
     */
    public static final String _2101 = "2101";

    /**
     * 蓄电池包总数
     */
    public static final String _2102 = "2102";

    /**
     * 单体温度值列表
     */
    public static final String SINGLE_TEMP = "2103";

    /**
     * 车速
     */
    public static final String SPEED = "2201";

    /**
     * 当前总里程, 定时任务关键字
     */
    public static final String TOTAL_MILEAGE = "2202";

    /**
     * GEARS 档位
     */
    public static final String GEARS = "2203";

    /**
     * braking force 制动力
     */
    public static final String BRAKING_FORCE="2204";

    /**
     * driving force 驱动力
     */
    public static final String DRIVING_FORCE="2205";

    /**
     * 加速踏板行程值
     */
    public static final String ACCELERATOR_PEDAL = "2208";

    /**
     * 制动踏板行程值
     */
    public static final String BRAKING_PEDAL = "2209";

    /**
     * 空调设定温度
     */
    public static final String _2210 = "2210";

    /**
     * SOC 过高告警
     */
    public static final String RUNNING_MODE="2213";

    /**
     * 续驶里程
     */
    public static final String _2290 = "2290";

    /**
     * 充放电状态
     */
    public static final String CHARGE_STATUS = "2301";

    /**
     * 电机控制器温度
     */
    public static final String DRIVING_ELE_MAC_TEMPCTOL = "2302";

    /**
     * 驱动电机转速
     */
    public static final String DRIVING_ELE_MAC_REV = "2303";

    /**
     * 驱动电机温度
     */
    public static final String DRIVING_ELE_MAC_TEMP = "2304";

    /**
     * 驱动电机输入电压
     */
    public static final String DRIVING_ELE_MAC_VOLT = "2305";

    /**
     * 驱动电机母线电流
     */
    public static final String DRIVING_ELE_MAC_ELE = "2306";

    /**
     * 驱动电机列表
     */
    public static final String DRIVING_ELE_MAC_LIST="2308";

    /**
     * 驱动电机序号
     */
    public static final String DRIVING_ELE_MAC_SEQ="2309";

    /**
     * 驱动电机状态
     */
    public static final String DRIVING_ELE_MAC_STATUS="2310";

    /**
     * 驱动电机转矩
     */
    public static final String DRIVING_ELE_MAC_TORQUE="2311";

    /**
     * 发动机状态
     */
    public static final String ENGINES = "2401";

    /**
     * ECU温度
     */
    public static final String _2402 = "2402";

    /**
     * 车辆电池电压
     */
    public static final String _2403 = "2403";

    /**
     * 发动机温度
     */
    public static final String _2404 = "2404";

    /**
     * 进气歧管气压
     */
    public static final String _2405 = "2405";

    /**
     * 进气温度
     */
    public static final String _2406 = "2406";

    /**
     * 废气排出温度
     */
    public static final String _2407 = "2407";

    /**
     * 燃料喷射压力
     */
    public static final String _2408 = "2408";

    /**
     * 燃料喷射量
     */
    public static final String _2409 = "2409";

    /**
     * 点火提前角
     */
    public static final String _2410 = "2410";

    /**
     * 曲轴转速
     */
    public static final String _2411 = "2411";

    /**
     * 油门开度
     */
    public static final String _2412 = "2412";

    /**
     * 定位状态
     */
    public static final String ORIENTATION = "2501";

    /**
     * 经度
     */
    public static final String LONGITUDE = "2502";

    /**
     * 维度
     */
    public static final String LATITUDE = "2503";

    /**
     * 速度
     */
    public static final String _2504 = "2504";

    /**
     * 方向
     */
    public static final String _2505 = "2505";

    /**
     * 最高电压动力蓄电池单体所在电池包序号, 最高电压电池子系统号
     */
    public static final String HIGHVOLT_CHILD_NUM = "2601";

    /**
     * 最高电压单体蓄电池序号, 最高电压电池单体代号
     */
    public static final String HIGHVOLT_SINGLE_NUM = "2602";

    /**
     * 电池单体电压最高值
     */
    public static final String SINGLE_VOLT_HIGN_VAL = "2603";

    /**
     * 最低电压动力蓄电池包序号, 最低电压电池子系统号
     */
    public static final String LOWVOLT_CHILD_NUM = "2604";

    /**
     * 最低电压单体蓄电池序号, 最低电压电池单体代号
     */
    public static final String LOWVOLT_SINGLE_NUM = "2605";

    /**
     * 电池单体电压最低值
     */
    public static final String SINGLE_VOLT_LOW_VAL = "2606";

    /**
     * 最高温度子系统号
     */
    public static final String HIGNTEMP_CHILD = "2607";

    /**
     * 最高温度探针单体代号
     */
    public static final String SINGLE_HIGNTEMP_NUM = "2608";

    /**
     * 电池单体最高温度值
     */
    public static final String SINGLE_HIGNTEMP_VAL = "2609";

    /**
     * 最低温度子系统号
     */
    public static final String LOWTEMP_CHILD = "2610";

    /**
     * 最低温度探针单体代号
     */
    public static final String SINGLE_LOWTEMP_NUM = "2611";

    /**
     * 电池单体最低温度值
     */
    public static final String SINGLE_LOWTEMP_VAL = "2612";

    /**
     * 总电压
     */
    public static final String TOTAL_VOLT = "2613";

    /**
     * 总电流
     */
    public static final String TOTAL_ELE = "2614";

    /**
     * SOC 电池剩余电量百分比
     */
    public static final String SOC = "2615";

    /**
     * 剩余能量
     */
    public static final String _2616 = "2616";

    /**
     * 绝缘电阻
     */
    public static final String INSULATION_RESISTANCE = "2617";

    /**
     * 自定义类型编码列表
     */
    public static final String _2701 = "2701";

    /**
     * 自定义类型数据列表
     */
    public static final String _2702 = "2702";

    /**
     * 动力蓄电池报警标志
     */
    public static final String POWER_BATTERY_ALARM_FLAG_2801 = "2801";

    /**
     * 动力蓄电池其他故障总数
     */
    public static final String _2802 = "2802";

    /**
     * 动力蓄电池其他故障代码列表
     */
    public static final String _2803 = "2803";

    /**
     * 电机故障总数
     */
    public static final String _2804 = "2804";

    /**
     * 电机故障代码列表
     */
    public static final String _2805 = "2805";

    /**
     * 发动机故障总数
     */
    public static final String _2806 = "2806";

    /**
     * 发动机故障列表
     */
    public static final String _2807 = "2807";

    /**
     * 其他故障总数
     */
    public static final String _2808 = "2808";

    /**
     * 其他故障代码列表
     */
    public static final String _2809 = "2809";

    /**
     * SOC 过高告警
     */
    public static final String SOC_HIGH_ALARM="2909";

    /**
     * 充电状态
     */
    public static final String ALARM_STATUS = "2920";
    public static final String 充电状态_2920 = "2920";
    public static final String _2920_充电状态 = "2920";

    /**
     * 车辆状态
     */
    public static final String CAR_STATUS="3201";

    /**
     * 通用报警标志值
     */
    public static final String ALARM_MARK="3801";

    /**
     * 单体电压原始报文
     */
    public static final String SINGLE_VOLT_ORIG="7003";

    /**
     * 单体文档原始报文
     */
    public static final String SINGLE_TEMP_ORGI="7103";

    /**
     * SOC2
     */
    public static final String _7615 = "7615";
}
